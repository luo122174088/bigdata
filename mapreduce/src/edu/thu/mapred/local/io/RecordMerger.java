package edu.thu.mapred.local.io;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.thu.mapred.local.LocalJobConf;
import edu.thu.mapred.local.map.CombineDriver;
import edu.thu.mapred.local.util.PriorityQueue;
import edu.thu.mapred.local.util.RecordComparator;

public class RecordMerger implements RawRecordIterator {

	private static Logger logger = LoggerFactory.getLogger(RecordMerger.class);

	public static RawRecordIterator merge(LocalJobConf conf, List<RecordSegment> segments,
			File tmpDir, int factor, RecordComparator comparator) throws IOException {
		return new RecordMerger(conf, segments, true, comparator).merge(tmpDir, factor);
	}

	private LocalJobConf conf;

	private PriorityQueue<FileSegment> fileSegments;
	private List<MemorySegment> memorySegments = new ArrayList<>();
	private RecordComparator comparator;
	private DataInputBuffer key;
	private DataInputBuffer value;
	private RecordSegment min;
	private CombineDriver driver;

	private Comparator<FileSegment> segmentComparator = new Comparator<FileSegment>() {
		@Override
		public int compare(FileSegment o1, FileSegment o2) {
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
			int s2 = key2.getPosition();

			return comparator.compare(key1.getData(), s1, key2.getData(), s2);
		}
	};
	private PriorityQueue<RecordSegment> queue = new PriorityQueue<>(recordComparator);

	public RecordMerger(LocalJobConf conf, List<RecordSegment> segments, boolean deleteInputs,
			RecordComparator comparator) throws IOException {
		this.conf = conf;
		this.comparator = comparator;
		this.fileSegments = new PriorityQueue<>(segmentComparator);
		this.fileSegments.initialize(segments.size());
		for (RecordSegment seg : segments) {
			if (seg instanceof FileSegment) {
				this.fileSegments.put((FileSegment) seg);
			} else {
				this.memorySegments.add((MemorySegment) seg);
			}
		}

		logger.debug("Merging {} memory segments, {} file segments.", memorySegments.size(),
				fileSegments.size());

		this.driver = new CombineDriver(conf);

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

	//only merge files
	RawRecordIterator merge(File tmpDir, int factor) throws IOException {
		int numSegments = this.fileSegments.size();
		logger.info("Merging {} file segments.", numSegments);
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
						logger.warn("Skipped invalid segment: {}", segment);
					}
				}

				if (numSegments <= factor) {
					if (memorySegments.size() > 0) {
						PriorityQueue<RecordSegment> newQueue = new PriorityQueue<>(recordComparator);
						newQueue.initialize(queue.size() + memorySegments.size());
						for (MemorySegment m : memorySegments) {
							m.init();
							m.next();
							newQueue.put(m);
						}
						RecordSegment s = null;
						while ((s = queue.pop()) != null) {
							newQueue.put(s);
						}
						queue = newQueue;
					}

					return this;
				} else {
					File outputFile = new File(tmpDir, "intermediate." + round);

					LocalRecordWriter writer = new LocalRecordWriter(this.conf, outputFile);

					writeFile(this, writer);
					writer.close();
					this.close();
					FileSegment tempSegment = new FileSegment(conf, outputFile, false);
					this.fileSegments.put(tempSegment);
					numSegments = this.fileSegments.size();
					round++;
				}
				factor = origFactor;
			}
		} finally {
			long end = System.currentTimeMillis();
			logger.info("Merge finishes in {} rounds, {}ms.", round, (end - start));
		}

	}

	private void writeFile(RawRecordIterator iterator, LocalRecordWriter writer) throws IOException {
		if (conf.getMergeCombine()) {
			driver.init(iterator, writer);
			driver.run();
		} else {
			while (iterator.next()) {
				writer.append(iterator.getKey(), iterator.getValue());
			}
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
		if (this.fileSegments.size() == 0) {
			return null;
		}
		RecordSegment segment = this.fileSegments.pop();
		return segment;
	}

}
