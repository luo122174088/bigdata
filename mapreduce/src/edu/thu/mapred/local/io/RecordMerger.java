package edu.thu.mapred.local.io;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.thu.mapred.local.LocalJobConf;
import edu.thu.mapred.local.util.PriorityQueue;
import edu.thu.mapred.local.util.RecordComparator;

public class RecordMerger implements RawRecordIterator {

	private static Logger logger = LoggerFactory.getLogger(RecordMerger.class);

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

	private PriorityQueue<RecordSegment> segments;;

	private RecordComparator comparator;

	private DataInputBuffer key;
	private DataInputBuffer value;
	private RecordSegment min;

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
		this.segments = new PriorityQueue<>(segmentComparator);
		this.segments.initialize(segments.size());
		for (RecordSegment seg : segments) {
			this.segments.put(seg);
		}

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

	public boolean next() throws IOException {
		if (queue.size() == 0) {
			return false;
		}
		if (min != null) {
			boolean hasNext = min.next();
			if (hasNext) {
				queue.adjustTop();
			} else {
				queue.pop();
				min.close();
				if (queue.size() == 0) {
					min = null;
					return false;
				}
			}
		}

		min = queue.top();
		key = min.getKey();
		value = min.getValue();

		return true;
	}

	RawRecordIterator merge(File tmpDir, int factor) throws IOException {
		int numSegments = this.segments.size();
		logger.info("Merging {} segments.", numSegments);
		long start = System.currentTimeMillis();
		int origFactor = factor;
		int round = 1;

		try {
			while (true) {
				factor = getRoundFactor(factor, round, numSegments);
				queue.initialize(factor);

				int num = 0;
				RecordSegment segment = null;
				while (num < factor && (segment = getRecordSegment()) != null) {
					segment.init();
					boolean hasNext = segment.next();
					if (hasNext) {
						queue.put(segment);
						num++;
					} else {
						segment.close();
						numSegments--;
						logger.warn("Skipped invalid segment: {}", segment.file.getName());
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
					this.segments.put(tempSegment);
					numSegments = this.segments.size();
					round++;
				}
				factor = origFactor;
			}
		} finally {
			long end = System.currentTimeMillis();
			logger.info("Merge finishes in {} rounds, {}ms.", round, (end - start));
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
		RecordSegment segment = this.segments.pop();
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
				boolean result = this.file.delete();
				if (!result) {
					logger.warn("Fail to delete " + file.getAbsolutePath());
				}
			}
		}
	}

}
