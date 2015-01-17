package edu.thu.mapred.local.io;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;

import edu.thu.mapred.local.LocalJobConf;
import edu.thu.mapred.local.util.PriorityQueue;
import edu.thu.mapred.local.util.RecordComparator;

public class RecordMerger implements RawRecordIterator {

	public static RawRecordIterator merge(List<RecordSegment> segments, File tmpDir, int factor,
			RecordComparator comparator) throws IOException {
		return new RecordMerger(segments, true, comparator).merge(tmpDir, factor);
	}

	public static <K extends Object, V extends Object> void writeFile(RawRecordIterator iterator,
			LocalRecordWriter writer) throws IOException {
		while (iterator.next()) {
			writer.append(iterator.getKey(), iterator.getValue());
		}
	}

	private LocalJobConf conf;

	private List<RecordSegment> segments = new LinkedList<>();

	private RecordComparator comparator;

	private DataInputBuffer key;
	private DataInputBuffer value;
	private Comparator<RecordSegment> segmentComparator = new Comparator<RecordSegment>() {
		@Override
		public int compare(RecordSegment o1, RecordSegment o2) {
			if (o1.getLength() == o2.getLength()) {
				return 0;
			}

			return o1.getLength() < o2.getLength() ? -1 : 1;
		}
	};

	private Comparator<RecordSegment> recordComparator = new Comparator<RecordSegment>() {
		public int compare(RecordSegment o1, RecordSegment o2) {
			DataInputBuffer key1 = o1.getKey();
			DataInputBuffer key2 = o2.getKey();
			int s1 = key1.getPosition();
			int l1 = key1.getLength() - s1;
			int s2 = key2.getPosition();
			int l2 = key2.getLength() - s2;

			return comparator.compare(key1.getData(), s1, l1, key2.getData(), s2, l2);
		}
	};

	private PriorityQueue<RecordSegment> queue = new PriorityQueue<>(recordComparator);

	public RecordMerger(List<RecordSegment> segments, boolean deleteInputs,
			RecordComparator comparator) throws IOException {
		this.comparator = comparator;

		for (RecordSegment seg : segments) {
			this.segments.add(seg);
		}

		Collections.sort(this.segments, this.segmentComparator);
	}

	@Override
	public void close() throws IOException {
		RecordSegment segment;
		if (queue != null) {
			while ((segment = queue.pop()) != null) {
				segment.close();
			}
		}
	}

	@Override
	public DataInputBuffer getKey() throws IOException {
		return this.key;
	}

	@Override
	public DataInputBuffer getValue() throws IOException {
		return this.value;
	}

	@Override
	public boolean next() throws IOException {
		if (queue.size() == 0) {
			return false;
		}

		RecordSegment segment = queue.top();
		this.key = segment.getKey();
		this.value = segment.getValue();

		boolean hasNext = segment.next();
		if (hasNext) {
			queue.adjustTop();
		} else {
			queue.pop();
			segment.close();
		}

		return true;
	}

	RawRecordIterator merge(File tmpDir, int factor) throws IOException {
		int numSegments = this.segments.size();
		int origFactor = factor;
		int round = 1;
		while (true) {
			factor = getRoundFactor(factor, round, numSegments);
			queue.initialize(factor);

			int num = 0;
			RecordSegment segment = null;
			while ((segment = getRecordSegment()) != null && num < factor) {
				segment.init();
				boolean hasNext = segment.next();
				if (hasNext) {
					queue.put(segment);
					num++;
				} else {
					segment.close();
					numSegments--;
				}
			}

			if (numSegments <= factor) {
				return this;
			} else {
				File outputFile = new File(tmpDir, "intermediate." + round);

				LocalRecordWriter writer = new LocalRecordWriter(this.conf, outputFile);
				writeFile(this, writer);
				writer.close();

				this.close();

				RecordSegment tempSegment = new RecordSegment(outputFile, false);
				this.segments.add(tempSegment);
				numSegments = this.segments.size();
				Collections.sort(this.segments, this.segmentComparator);
				round++;
			}
			factor = origFactor;
		}

	}

	private int getRoundFactor(int factor, int round, int numSegments) {
		if (round > 1 || numSegments <= factor || factor == 1) {
			return factor;
		}
		int mod = (numSegments - 1) % (factor - 1);
		if (mod == 0) {
			return factor;
		}
		return mod + 1;
	}

	private RecordSegment getRecordSegment() {
		if (this.segments.size() == 0) {
			return null;
		}
		RecordSegment segment = this.segments.remove(0);
		return segment;
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
			if (this.reader == null) {
				this.reader = new LocalRecordReader(this.file);
			}
		}

		DataInputBuffer getKey() {
			return this.key;
		}

		DataInputBuffer getValue() {
			return this.value;
		}

		long getLength() {
			return this.file.length();
		}

		boolean next() throws IOException {
			return this.reader.next(this.key, this.value);
		}

		void close() throws IOException {
			this.reader.close();

			if (!this.preserve) {
				this.file.delete();
			}
		}
	}

}
