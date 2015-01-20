package edu.thu.mapred.local.map;

import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.TaskId;

import edu.thu.mapred.local.LocalJobConf;
import edu.thu.mapred.local.LocalRecord;
import edu.thu.mapred.local.io.DataInputBuffer;
import edu.thu.mapred.local.io.FileSegment;
import edu.thu.mapred.local.io.LocalRecordWriter;
import edu.thu.mapred.local.io.RawRecordIterator;
import edu.thu.mapred.local.io.RecordMerger;
import edu.thu.mapred.local.io.RecordSegment;
import edu.thu.mapred.local.io.TaskFileHelper;
import edu.thu.mapred.local.util.IndexSorter;
import edu.thu.mapred.local.util.IndexSorter.IndexSortable;
import edu.thu.mapred.local.util.RecordComparator;

class MapOutputCollector implements OutputCollector, IndexSortable {
	private static Logger logger = LoggerFactory.getLogger(MapOutputCollector.class);

	private static final int KEY = 0;
	private static final int VALUE = 1;
	private static final int KVSIZE = 2;
	private static final int IDXSIZE = (KVSIZE + 1) * 4;

	private volatile int rec_start = 0;
	private volatile int rec_end = 0;
	private int rec_index = 0;
	private volatile int buf_start = 0;
	private volatile int buf_end = 0;
	private volatile int buf_void = 0;
	private int buf_index = 0;
	private int buf_mark = 0;
	private int[] rec_offsets;
	private int[] rec_indices;
	private byte[] rec_buffer;

	private volatile int numSpills = 0;
	private volatile Exception spillException = null;

	private final float idxper;
	private final int sortmb;
	private final float spillper;

	private final int softIndexLimit;
	private final int softBufferLimit;

	private final ReentrantLock spillLock = new ReentrantLock();
	private final Condition spillDone = this.spillLock.newCondition();
	private final Condition spillReady = this.spillLock.newCondition();
	private final MapDataOutput outBuffer = new MapDataOutput();
	private volatile boolean spillRunning = false;
	private SpillThread spillThread;

	private LocalJobConf conf;
	private RecordComparator comparator;
	private TaskId id;
	private TaskFileHelper fileHelper;
	private List<RecordSegment> mapFiles;

	public MapOutputCollector(LocalJobConf conf, TaskFileHelper fileHelper,
			List<RecordSegment> mapFiles) throws Exception {
		this.conf = conf;
		this.fileHelper = fileHelper;
		this.mapFiles = mapFiles;

		this.idxper = conf.getIndexPer();
		this.spillper = conf.getSpiller();
		this.sortmb = conf.getSortMB();

		int maxMemUsage = this.sortmb * 1024 * 1024;
		int recordCapacity = (int) (maxMemUsage * this.idxper);
		recordCapacity -= recordCapacity % IDXSIZE;
		this.rec_buffer = new byte[maxMemUsage - recordCapacity];
		this.buf_void = this.rec_buffer.length;
		recordCapacity /= IDXSIZE;
		this.rec_offsets = new int[recordCapacity];
		this.rec_indices = new int[recordCapacity * KVSIZE];
		this.softBufferLimit = (int) (this.rec_buffer.length * this.spillper);
		this.softIndexLimit = (int) (this.rec_offsets.length * this.spillper);

		this.comparator = conf.getMapOutputKeyComparator();
	}

	public void init(TaskId id) throws Exception {
		this.id = id;
		this.rec_start = 0;
		this.rec_end = 0;
		this.rec_index = 0;
		this.buf_start = 0;
		this.buf_end = 0;
		this.buf_index = 0;
		this.buf_mark = 0;
		this.numSpills = 0;
		this.spillException = null;
		this.buf_void = this.rec_buffer.length;

		// start spillThread
		this.spillThread = new SpillThread();
		this.spillThread.setDaemon(true);
		this.spillThread.setName("SpillThread");
		this.spillLock.lock();
		try {
			this.spillThread.start();
			// wait for spill to start
			while (!this.spillRunning) {
				this.spillDone.await();
			}
		} catch (InterruptedException e) {
			throw e;
		} finally {
			this.spillLock.unlock();
		}

	}

	public synchronized void collect(Record key, Record value) throws IOException {
		final int rec_next = (this.rec_index + 1) % this.rec_offsets.length;
		checkIndexFull(rec_next);
		try {
			int key_start = this.buf_index;
			((LocalRecord) key).serialize(this.outBuffer);
			if (this.buf_index < key_start) {
				// reset
				this.outBuffer.reset();
				key_start = 0;
			}

			int val_start = this.buf_index;
			((LocalRecord) value).serialize(this.outBuffer);
			this.outBuffer.mark();

			int index = this.rec_index * KVSIZE;
			this.rec_offsets[this.rec_index] = index;
			this.rec_indices[index + KEY] = key_start;
			this.rec_indices[index + VALUE] = val_start;
			this.rec_index = rec_next;
		} catch (BufferTooSmallException e) {
			spillRecord(key, value);
		}
	}

	private void checkIndexFull(int rec_next) throws IOException {
		this.spillLock.lock();
		try {
			while (true) {
				if (this.spillException != null) {
					throw new IOException(this.spillException);
				}
				boolean full = (rec_next == this.rec_start);
				boolean softlimit;
				if (rec_next > this.rec_end) {
					softlimit = rec_next - this.rec_end > this.softIndexLimit;
				} else {
					softlimit = this.rec_end - rec_next <= this.rec_offsets.length - this.softIndexLimit;
				}
				if (this.rec_start == this.rec_end && softlimit) {
					startSpill();
				}
				if (full) {
					// index full, wait for spill to finish
					logger.debug("Map task {} output collector full, wait for spill to finish.", id);
					try {
						while (this.rec_start != this.rec_end) {
							this.spillDone.await();
						}
					} catch (InterruptedException e) {
						throw new IOException(e);
					}
				} else {
					// continue to write
					break;
				}
			}
		} finally {
			this.spillLock.unlock();
		}
	}

	@Override
	public int compareIndex(int i, int j) {
		int ii = this.rec_offsets[i % this.rec_offsets.length];
		int ij = this.rec_offsets[j % this.rec_offsets.length];
		return this.comparator.compare(this.rec_buffer, this.rec_indices[ii + KEY], this.rec_buffer,
				this.rec_indices[ij + KEY]);
	}

	@Override
	public void swapIndex(int i, int j) {
		i %= this.rec_offsets.length;
		j %= this.rec_offsets.length;

		int tmp = this.rec_offsets[i];
		this.rec_offsets[i] = this.rec_offsets[j];
		this.rec_offsets[j] = tmp;
	}

	private class MapOutputBuffer extends OutputStream {
		private final byte[] bytes = new byte[1];

		@Override
		public synchronized void write(int v) throws IOException {
			this.bytes[0] = (byte) v;
			write(this.bytes, 0, 1);
		}

		@Override
		public synchronized void write(byte b[], int off, int len) throws IOException {
			boolean ful = false;
			boolean wrap = false;
			spillLock.lock();
			try {
				while (true) {
					if (spillException != null) {
						throw new IOException(spillException);
					}
					if (buf_start <= buf_end && buf_end <= buf_index) {
						ful = buf_index + len > buf_void;
						wrap = (buf_void - buf_index) + buf_start > len;
					} else {
						wrap = false;
						ful = buf_index + len > buf_start;
					}

					if (rec_start == rec_end) {
						if (rec_end != rec_index) {
							boolean bufsoftlimit;
							if (buf_index > buf_end) {
								bufsoftlimit = buf_index - buf_end > softBufferLimit;
							} else {
								bufsoftlimit = buf_end - buf_index < buf_void - softBufferLimit;
							}
							if (bufsoftlimit || (ful && !wrap)) {
								startSpill();
							}
						} else if (ful && !wrap) {
							final int size = ((buf_end <= buf_index) ? buf_index - buf_end : (buf_void - buf_end)
									+ buf_index)
									+ len;
							buf_start = buf_end = buf_index = buf_mark = 0;
							rec_start = rec_end = rec_index = 0;
							buf_void = rec_buffer.length;
							throw new BufferTooSmallException(size + " bytes");
						}
					}
					if (ful && !wrap) {
						try {
							while (rec_start != rec_end) {
								spillDone.await();
							}
						} catch (InterruptedException e) {
							throw new IOException(e);
						}
					} else {
						break;
					}
				}
			} finally {
				spillLock.unlock();
			}
			if (ful) {
				final int gaplen = buf_void - buf_index;
				System.arraycopy(b, off, rec_buffer, buf_index, gaplen);
				len -= gaplen;
				off += gaplen;
				buf_index = 0;
			}
			System.arraycopy(b, off, rec_buffer, buf_index, len);
			buf_index += len;
		}
	}

	protected class MapDataOutput extends DataOutputStream {

		public MapDataOutput() {
			this(new MapOutputBuffer());
		}

		private MapDataOutput(OutputStream out) {
			super(out);
		}

		public int mark() {
			buf_mark = buf_index;
			return buf_index;
		}

		// move the key at the end to the head
		public synchronized void reset() throws IOException {
			int headlen = buf_void - buf_mark;
			buf_void = buf_mark;
			if (buf_index + headlen < buf_start) {
				// enough space
				System.arraycopy(rec_buffer, 0, rec_buffer, headlen, buf_index);
				System.arraycopy(rec_buffer, buf_void, rec_buffer, 0, headlen);
				buf_index += headlen;
			} else {
				byte[] tmp = new byte[buf_index];
				System.arraycopy(rec_buffer, 0, tmp, 0, buf_index);
				buf_index = 0;
				out.write(rec_buffer, buf_mark, headlen);
				out.write(tmp);
			}
		}
	}

	public synchronized void flush() throws IOException {
		this.spillLock.lock();
		try {
			while (this.rec_start != this.rec_end) {
				this.spillDone.await();
			}
			if (this.spillException != null) {
				throw new IOException(this.spillException);
			}
			if (this.rec_end != this.rec_index) {
				this.rec_end = this.rec_index;
				this.buf_end = this.buf_mark;
				spill();
			}
		} catch (InterruptedException e) {
			throw new IOException(e);
		} finally {
			this.spillLock.unlock();
		}

		try {
			this.spillThread.interrupt();
			this.spillThread.join();
			this.spillThread = null;
		} catch (InterruptedException e) {
			throw new IOException(e);
		}
		// kvbuffer = null;
		// mergeParts();

		// for (int i = 0; i < this.numSpills; i++) {
		// mapFiles.add(fileHelper.getSpillFile(i));
		// }
	}

	// postpone to reduce phase
	private void mergeParts() throws Exception {
		final File[] files = new File[this.numSpills];

		for (int i = 0; i < this.numSpills; i++) {
			files[i] = this.fileHelper.getSpillFile(i);
		}
		File finalOutputFile = this.fileHelper.getOutputFile();

		if (this.numSpills == 1) {
			files[0].renameTo(finalOutputFile);
			this.mapFiles.add(new FileSegment(conf, finalOutputFile, false));
			return;
		}

		if (this.numSpills == 0) {

			finalOutputFile.createNewFile();
			return;
		}
		List<RecordSegment> segments = new ArrayList<RecordSegment>(this.numSpills);
		for (int i = 0; i < this.numSpills; i++) {
			RecordSegment s = new FileSegment(conf, files[i], false);
			segments.add(i, s);
		}

		RawRecordIterator kvIter = RecordMerger.merge(conf, segments, this.fileHelper.getTempDir(),
				this.conf.getSortFactor(), this.comparator);

		LocalRecordWriter writer = new LocalRecordWriter(this.conf, finalOutputFile);
		CombineDriver driver = new CombineDriver(this.conf);
		driver.init(kvIter, writer);
		driver.run();
		this.mapFiles.add(new FileSegment(conf, finalOutputFile, false));
		writer.close();

		for (int i = 0; i < this.numSpills; i++) {
			files[i].delete();
		}
	}

	public void close() {
	}

	protected class SpillThread extends Thread {
		@Override
		public void run() {
			spillLock.lock();
			spillRunning = true;
			try {
				spillDone.signal();
				while (true) {
					while (rec_start == rec_end) {
						// wait for writer
						spillReady.await();
					}
					try {
						spillLock.unlock();
						spill();
						logger.info("Map task {} finishes spill.", id);
					} catch (Exception e) {
						spillException = e;
					} finally {
						spillLock.lock();
						if (buf_end < buf_index && buf_index < buf_start) {
							buf_void = rec_buffer.length;
						}
						rec_start = rec_end;
						buf_start = buf_end;
						// inform writer
						spillDone.signal();
					}
				}
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			} finally {
				spillLock.unlock();
				spillRunning = false;
			}
		}
	}

	private synchronized void startSpill() {
		logger.debug("Map task {} start spill.", id);
		logger.debug("rec_start = {}, rec_end = {}, length = {}", rec_start, rec_index,
				rec_offsets.length);
		logger.debug("buf_start = {}, buf_end = {}, bufvoid = {}", buf_start, buf_mark, buf_void);

		this.rec_end = this.rec_index;
		this.buf_end = this.buf_mark;
		this.spillReady.signal();
	}

	private void spill() throws IOException {
		File file = this.fileHelper.getSpillFile(this.numSpills);
		int end = 0;
		if (this.rec_end > this.rec_start) {
			end = this.rec_end;
		} else {
			end = this.rec_offsets.length + this.rec_end;
		}
		IndexSorter.sort(MapOutputCollector.this, this.rec_start, end);
		int sp_index = this.rec_start;
		LocalRecordWriter writer = null;
		try {
			writer = new LocalRecordWriter(this.conf, file);
			int sp_start = sp_index;
			sp_index = end;
			RawRecordIterator kvIter = new MapResultIterator(sp_start, sp_index);
			CombineDriver driver = new CombineDriver(this.conf);
			driver.init(kvIter, writer);
			driver.run();
		} finally {
			if (writer != null) {
				writer.close();
			}
			this.mapFiles.add(new FileSegment(conf, file, false));
		}
		this.numSpills++;
	}

	private void spillRecord(final Record key, final Record value) throws IOException {
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
		this.numSpills++;
	}

	protected class RingInputBuffer extends DataInputBuffer {
		private byte[] buffer;
		private int start;
		private int length;

		@Override
		public void reset(byte[] buffer, int start, int length) {
			this.buffer = buffer;
			this.start = start;
			this.length = length;

			if (start + length > buf_void) {
				this.buffer = new byte[this.length];
				int taillen = buf_void - start;
				System.arraycopy(buffer, start, this.buffer, 0, taillen);
				System.arraycopy(buffer, 0, this.buffer, taillen, length - taillen);
				this.start = 0;
			}

			super.reset(this.buffer, this.start, this.length);
		}
	}

	protected class MapResultIterator implements RawRecordIterator {
		private final DataInputBuffer key = new DataInputBuffer();
		private final DataInputBuffer value = new RingInputBuffer();
		private final int end;
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
			final int kvoff = rec_offsets[this.current % rec_offsets.length];
			this.key.reset(rec_buffer, rec_indices[kvoff + KEY], rec_indices[kvoff + VALUE]
					- rec_indices[kvoff + KEY]);
			return this.key;
		}

		@Override
		public DataInputBuffer getValue() throws IOException {
			getValueBuffer(rec_offsets[this.current % rec_offsets.length], this.value);
			return this.value;
		}

		@Override
		public void close() {
		}
	}

	private void getValueBuffer(int offset, DataInputBuffer value) {
		int next_index = 0;
		int value_len = 0;
		if (offset / KVSIZE == (this.rec_end - 1 + this.rec_offsets.length) % this.rec_offsets.length) {
			next_index = this.buf_end;
		} else {
			next_index = this.rec_indices[(offset + KVSIZE + KEY) % this.rec_indices.length];
		}
		if (next_index >= this.rec_indices[offset + VALUE]) {
			value_len = next_index - this.rec_indices[offset + VALUE];
		} else {
			value_len = (this.buf_void - this.rec_indices[offset + VALUE]) + next_index;
		}
		value.reset(this.rec_buffer, this.rec_indices[offset + VALUE], value_len);
	}

	private static class BufferTooSmallException extends IOException {

		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;

		public BufferTooSmallException(String msg) {
			super(msg);
		}

	}

}
