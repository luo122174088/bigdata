package edu.thu.mapred.local.io;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import edu.thu.mapred.local.LocalJobConf;
import edu.thu.mapred.local.RawRecordComparator;
import edu.thu.mapred.local.RawRecordIterator;
import edu.thu.mapred.local.util.PriorityQueue;

public class RecordMerger {

	public static RawRecordIterator merge(List<RecordSegment> segments, File tmpDir, int factor,
			RawRecordComparator comparator) throws IOException {
		return new MergeQueue(segments, true, comparator).merge(tmpDir, factor);

	}

	public static <K extends Object, V extends Object> void writeFile(RawRecordIterator iterator,
			LocalRecordWriter writer) throws IOException {
		while (iterator.next()) {
			writer.append(iterator.getKey(), iterator.getValue());
		}
	}

	public static class RecordSegment {
		LocalRecordReader reader = null;
		DataInputBuffer key = new DataInputBuffer();
		DataInputBuffer value = new DataInputBuffer();

		File file = null;
		boolean preserve = false;

		public RecordSegment(File file, boolean preserve) throws IOException {
			this.file = file;
			this.preserve = preserve;
		}

		private void init() throws IOException {
			if (reader == null) {
				reader = new LocalRecordReader(file);
			}
		}

		DataInputBuffer getKey() {
			return key;
		}

		DataInputBuffer getValue() {
			return value;
		}

		long getLength() {
			return file.length();
		}

		boolean next() throws IOException {
			return reader.next(key, value);
		}

		void close() throws IOException {
			reader.close();

			if (!preserve) {
				file.delete();
			}
		}
	}

	private static class MergeQueue extends PriorityQueue<RecordSegment> implements RawRecordIterator {
		LocalJobConf conf;

		List<RecordSegment> segments = new ArrayList<>();

		RecordSegment minSegment;

		RawRecordComparator comparator;

		DataInputBuffer key;
		DataInputBuffer value;

		Comparator<RecordSegment> segmentComparator = new Comparator<RecordSegment>() {
			public int compare(RecordSegment o1, RecordSegment o2) {
				if (o1.getLength() == o2.getLength()) {
					return 0;
				}

				return o1.getLength() < o2.getLength() ? -1 : 1;
			}
		};

		public MergeQueue(List<RecordSegment> segments, boolean deleteInputs,
				RawRecordComparator comparator) throws IOException {
			this.comparator = comparator;

			for (RecordSegment seg : segments) {
				this.segments.add(seg);
			}

			Collections.sort(this.segments, segmentComparator);
		}

		public void close() throws IOException {
			RecordSegment segment;
			while ((segment = pop()) != null) {
				segment.close();
			}
		}

		public DataInputBuffer getKey() throws IOException {
			return key;
		}

		public DataInputBuffer getValue() throws IOException {
			return value;
		}

		private void adjustPriorityQueue(RecordSegment segment) throws IOException {
			boolean hasNext = segment.next();
			if (hasNext) {
				adjustTop();
			} else {
				pop();
				segment.close();
			}
		}

		public boolean next() throws IOException {
			if (size() == 0)
				return false;

			if (minSegment != null) {
				adjustPriorityQueue(minSegment);
				if (size() == 0) {
					minSegment = null;
					return false;
				}
			}
			minSegment = top();

			key = minSegment.getKey();
			value = minSegment.getValue();

			return true;
		}

		protected boolean lessThan(Object a, Object b) {
			DataInputBuffer key1 = ((RecordSegment) a).getKey();
			DataInputBuffer key2 = ((RecordSegment) b).getKey();
			int s1 = key1.getPosition();
			int l1 = key1.getLength() - s1;
			int s2 = key2.getPosition();
			int l2 = key2.getLength() - s2;

			return comparator.compare(key1.getData(), s1, l1, key2.getData(), s2, l2) < 0;
		}

		RawRecordIterator merge(File tmpDir, int factor) throws IOException {
			int numSegments = segments.size();
			int origFactor = factor;
			int passNo = 1;
			do {
				factor = getPassFactor(factor, passNo, numSegments);
				List<RecordSegment> segmentsToMerge = new ArrayList<>();
				int numSegmentsToConsider = factor;
				List<RecordSegment> mStream = getSegmentDescriptors(numSegmentsToConsider);
				for (RecordSegment segment : mStream) {
					segment.init();
					segment.next();
					segmentsToMerge.add(segment);
				}

				initialize(segmentsToMerge.size());
				clear();
				for (RecordSegment segment : segmentsToMerge) {
					put(segment);
				}

				if (numSegments <= factor) {
					return this;
				} else {
					File outputFile = new File(tmpDir, "intermediate." + passNo);

					LocalRecordWriter writer = new LocalRecordWriter(conf, outputFile);
					writeFile(this, writer);
					writer.close();

					this.close();

					RecordSegment tempSegment = new RecordSegment(outputFile, false);
					segments.add(tempSegment);
					numSegments = segments.size();
					Collections.sort(segments, segmentComparator);
					passNo++;
				}
				factor = origFactor;
			} while (true);
		}

		private int getPassFactor(int factor, int passNo, int numSegments) {
			if (passNo > 1 || numSegments <= factor || factor == 1)
				return factor;
			int mod = (numSegments - 1) % (factor - 1);
			if (mod == 0)
				return factor;
			return mod + 1;
		}

		private List<RecordSegment> getSegmentDescriptors(int numDescriptors) {
			if (numDescriptors > segments.size()) {
				List<RecordSegment> subList = new ArrayList<RecordSegment>(segments);
				segments.clear();
				return subList;
			}

			List<RecordSegment> subList = new ArrayList<RecordSegment>(
					segments.subList(0, numDescriptors));
			for (int i = 0; i < numDescriptors; ++i) {
				segments.remove(0);
			}
			return subList;
		}

	}
}
