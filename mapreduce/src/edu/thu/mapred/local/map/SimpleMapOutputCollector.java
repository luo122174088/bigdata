package edu.thu.mapred.local.map;

import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.TaskId;

import edu.thu.mapred.local.LocalJobConf;
import edu.thu.mapred.local.LocalRecord;
import edu.thu.mapred.local.io.DataInputBuffer;
import edu.thu.mapred.local.io.FileSegment;
import edu.thu.mapred.local.io.LocalRecordWriter;
import edu.thu.mapred.local.io.MemorySegment;
import edu.thu.mapred.local.io.RawRecordIterator;
import edu.thu.mapred.local.io.RecordSegment;
import edu.thu.mapred.local.io.TaskFileHelper;
import edu.thu.mapred.local.util.IndexSorter;
import edu.thu.mapred.local.util.IndexSorter.IndexSortable;
import edu.thu.mapred.local.util.RecordComparator;

public class SimpleMapOutputCollector extends DataOutputStream implements OutputCollector,
		IndexSortable {
	private static Logger logger = LoggerFactory.getLogger(MapOutputCollector.class);

	private static final int IDXSIZE = 1 * 4;

	private int rec_end = 0;
	private int buf_end = 0;
	private int[] rec_indices;
	private byte[] rec_buffer;

	private int numSpills = 0;

	private final float idxper;
	private final int sortmb;

	private SpillBuffer spillBuffer;

	private LocalJobConf conf;
	private RecordComparator comparator;
	private TaskId id;
	private TaskFileHelper fileHelper;
	private List<RecordSegment> mapFiles;

	public SimpleMapOutputCollector(LocalJobConf conf, TaskFileHelper fileHelper,
			List<RecordSegment> mapFiles) throws Exception {
		super(null);
		this.conf = conf;
		this.fileHelper = fileHelper;
		this.mapFiles = mapFiles;

		this.idxper = conf.getIndexPer();
		this.sortmb = conf.getSortMB();
		int maxMemUsage = this.sortmb * 1024 * 1024;
		int recordCapacity = (int) (maxMemUsage * this.idxper);
		recordCapacity -= recordCapacity % IDXSIZE;
		this.rec_buffer = new byte[maxMemUsage - recordCapacity];
		recordCapacity /= IDXSIZE;
		this.rec_indices = new int[recordCapacity];

		this.comparator = conf.getMapOutputKeyComparator();

		this.spillBuffer = new SpillBuffer(conf);

		this.out = new MapOutputStream();
	}

	public void init(TaskId id) {
		this.id = id;
		this.rec_end = 0;
		this.buf_end = 0;
		this.numSpills = 0;
	}

	public void collect(Record key, Record value) throws IOException {
		if (this.rec_end == this.rec_indices.length) {
			logger.debug("Map task {} record index full, start to spill.", id);
			spill();
		}
		try {
			int key_start = this.buf_end;
			((LocalRecord) key).serialize(this);
			((LocalRecord) value).serialize(this);
			this.rec_indices[this.rec_end] = key_start;
			this.rec_end++;
		} catch (ArrayIndexOutOfBoundsException e) {
			//out of buffer, spill.
			logger.debug("Map task {} buffer full, start to spill.", id);
			spill();
			try {
				int key_start = this.buf_end;
				((LocalRecord) key).serialize(this);
				((LocalRecord) value).serialize(this);
				this.rec_indices[this.rec_end] = key_start;
				this.rec_end++;
			} catch (ArrayIndexOutOfBoundsException e2) {
				//should not happen
				spillRecord(key, value);
			}

		}
	}

	@Override
	public int compareIndex(int i, int j) {
		return this.comparator.compare(this.rec_buffer, this.rec_indices[i], this.rec_buffer,
				this.rec_indices[j]);
	}

	@Override
	public void swapIndex(int i, int j) {
		int tmp = this.rec_indices[i];
		this.rec_indices[i] = this.rec_indices[j];
		this.rec_indices[j] = tmp;
	}

	private void spill() throws IOException {
		logger.debug("Map task {} start spill.", id);
		logger.debug("rec_start = {}, rec_end = {}", 0, rec_end);
		logger.debug("buf_start = {}, buf_end = {}", 0, buf_end);

		RecordSegment segment = null;
		LocalRecordWriter writer = null;
		IndexSorter.sort(this, 0, this.rec_end);

		if (!spillBuffer.full) {
			if (numSpills > 0) {
				int average = spillBuffer.buf_end / numSpills;
				if (spillBuffer.buf_end + average >= spillBuffer.buffer.length) {
					spillBuffer.full = true;
				}
			}
		}

		if (!spillBuffer.full) {
			//spill to buffer
			logger.debug("Map task {} spill buffer available.", id);
			writer = new LocalRecordWriter(this.conf, spillBuffer);
		} else {
			logger.debug("Map task {} spill buffer full, fall back to file.", id);
			File file = this.fileHelper.getSpillFile(this.numSpills);
			segment = new FileSegment(conf, file, false);
			writer = new LocalRecordWriter(this.conf, file);
		}

		try {
			RawRecordIterator kvIter = new MapResultIterator(0, rec_end);
			CombineDriver driver = new CombineDriver(this.conf);
			driver.init(kvIter, writer);
			driver.run();
		} catch (ArrayIndexOutOfBoundsException e) {
			//buffer full
			spillBuffer.full = true;
			logger.info("Map task {} spill buffer full during writing, fall back to file.", id);
			File file = this.fileHelper.getSpillFile(this.numSpills);
			segment = new FileSegment(conf, file, false);
			writer = new LocalRecordWriter(this.conf, file);
			RawRecordIterator kvIter = new MapResultIterator(0, rec_end);
			CombineDriver driver = new CombineDriver(this.conf);
			driver.init(kvIter, writer);
			driver.run();
		} finally {
			if (writer != null) {
				writer.close();
			}
		}

		if (!spillBuffer.full) {
			segment = new MemorySegment(conf, spillBuffer.buffer, spillBuffer.buf_start,
					spillBuffer.buf_end);
			logger.debug("Map task {} spill buffer used: {}, left: {}", id, spillBuffer.buf_end,
					spillBuffer.buffer.length - spillBuffer.buf_end);
			spillBuffer.mark();
		}

		this.mapFiles.add(segment);
		this.rec_end = 0;
		this.buf_end = 0;
		this.numSpills++;

	}

	private void spillRecord(final Record key, final Record value) throws IOException {
		logger.warn("Map task {} spills single record.", id);
		File file = this.fileHelper.getSpillFile(this.numSpills);
		LocalRecordWriter writer = null;
		try {
			writer = new LocalRecordWriter(this.conf, file);
			writer.append(key, value);
		} finally {
			if (null != writer) {
				writer.close();
			}
		}
		this.buf_end = 0;
		this.mapFiles.add(new FileSegment(conf, file, false));
		this.numSpills++;
	}

	public void flush() throws IOException {
		if (this.rec_end != 0) {
			logger.debug("Map task start to flush.", id);
			spill();
		}
		// kvbuffer = null;
		// mergeParts();

		// for (int i = 0; i < this.numSpills; i++) {
		// mapFiles.add(fileHelper.getSpillFile(i));
		// }
	}

	public void close() {
		//do nothing
		this.rec_buffer = null;
		this.rec_indices = null;
	}

	private class MapOutputStream extends OutputStream {

		@Override
		public synchronized void write(int v) throws IOException {
			rec_buffer[buf_end++] = (byte) v;

		}

		@Override
		public synchronized void write(byte b[], int off, int len) throws IOException {
			System.arraycopy(b, off, rec_buffer, buf_end, len);
			buf_end += len;
		}
	}

	protected class MapResultIterator implements RawRecordIterator {
		private DataInputBuffer key = new DataInputBuffer();
		private DataInputBuffer value = new DataInputBuffer();
		private int end;
		private int current;

		public MapResultIterator(int start, int end) {
			this.end = end;
			this.current = start - 1;
		}

		@Override
		public boolean next() throws IOException {
			return ++this.current < this.end;
		}

		@Override
		public DataInputBuffer getKey() throws IOException {
			this.key.reset(rec_buffer, rec_indices[this.current], buf_end);
			return this.key;
		}

		@Override
		public DataInputBuffer getValue() throws IOException {
			this.value.reset(rec_buffer, key.getPosition(), buf_end);
			return this.value;
		}

		@Override
		public void close() {
		}
	}

}
