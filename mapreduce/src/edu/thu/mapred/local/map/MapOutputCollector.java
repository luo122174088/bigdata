package edu.thu.mapred.local.map;

import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.TaskId;

import edu.thu.mapred.local.LocalJobConf;
import edu.thu.mapred.local.LocalRecord;
import edu.thu.mapred.local.io.DataInputBuffer;
import edu.thu.mapred.local.io.IndexSorter;
import edu.thu.mapred.local.io.IndexSorter.Sortable;
import edu.thu.mapred.local.io.LocalRecordWriter;
import edu.thu.mapred.local.io.RawRecordIterator;
import edu.thu.mapred.local.io.RecordMerger;
import edu.thu.mapred.local.io.RecordMerger.RecordSegment;
import edu.thu.mapred.local.io.TaskFileHelper;
import edu.thu.mapred.local.util.RecordComparator;

class MapOutputCollector implements Sortable {
	private final LocalJobConf conf;

	private RecordComparator comparator;

	private boolean combine = false;

	private volatile int kvstart = 0;
	private volatile int kvend = 0;
	private int kvindex = 0;
	private final int[] kvoffsets;
	private final int[] kvindices;
	private volatile int bufstart = 0;
	private volatile int bufend = 0;
	private volatile int bufvoid = 0;

	private int bufindex = 0;
	private int bufmark = 0;
	private byte[] kvbuffer;
	private static final int KEYSTART = 0;
	private static final int VALSTART = 1;
	private static final int ACCTSIZE = 2;
	private static final int RECSIZE = (ACCTSIZE + 1) * 4;

	private volatile int numSpills = 0;
	private volatile Exception sortSpillException = null;

	private final float spillper = 0.8f;
	private final float recper = 0.05f;
	private final int sortmb = 100;

	private final int softRecordLimit;
	private final int softBufferLimit;
	private final int minSpillsForCombine = 3;

	private final IndexSorter sorter = new IndexSorter();
	private final ReentrantLock spillLock = new ReentrantLock();
	private final Condition spillDone = this.spillLock.newCondition();
	private final Condition spillReady = this.spillLock.newCondition();
	private final MapDataOutput outBuffer = new MapDataOutput();
	private volatile boolean spillThreadRunning = false;
	private SpillThread spillThread;

	private TaskId id;
	private final TaskFileHelper fileHelper;
	private final List<File> mapFiles;

	public MapOutputCollector(LocalJobConf conf, TaskFileHelper fileHelper, List<File> mapFiles)
			throws Exception {
		this.conf = conf;
		this.fileHelper = fileHelper;
		this.mapFiles = mapFiles;

		int maxMemUsage = this.sortmb << 20;
		int recordCapacity = (int) (maxMemUsage * this.recper);
		recordCapacity -= recordCapacity % RECSIZE;
		this.kvbuffer = new byte[maxMemUsage - recordCapacity];
		this.bufvoid = this.kvbuffer.length;
		recordCapacity /= RECSIZE;
		this.kvoffsets = new int[recordCapacity];
		this.kvindices = new int[recordCapacity * ACCTSIZE];
		this.softBufferLimit = (int) (this.kvbuffer.length * this.spillper);
		this.softRecordLimit = (int) (this.kvoffsets.length * this.spillper);

		this.comparator = conf.getMapOutputKeyComparator();
		this.combine = conf.getCombinerClass() != null;

	}

	public void init(TaskId id) throws Exception {
		this.id = id;

		this.kvstart = 0;
		this.kvend = 0;
		this.kvindex = 0;
		this.bufindex = 0;
		this.bufmark = 0;
		this.numSpills = 0;
		this.sortSpillException = null;

		this.spillThread = new SpillThread();
		this.spillThread.setDaemon(true);
		this.spillThread.setName("SpillThread");
		this.spillLock.lock();
		try {
			this.spillThread.start();
			while (!this.spillThreadRunning) {
				this.spillDone.await();
			}
		} catch (InterruptedException e) {
			throw e;
		} finally {
			this.spillLock.unlock();
		}

	}

	public synchronized void collect(Record key, Record value) throws IOException {
		final int kvnext = (this.kvindex + 1) % this.kvoffsets.length;

		checkFull(kvnext);
		try {

			int keystart = this.bufindex;
			serialize(key);
			if (this.bufindex < keystart) {

				this.outBuffer.reset();
				keystart = 0;
			}

			final int valstart = this.bufindex;
			serialize(value);
			this.outBuffer.mark();

			int ind = this.kvindex * ACCTSIZE;
			this.kvoffsets[this.kvindex] = ind;
			this.kvindices[ind + KEYSTART] = keystart;
			this.kvindices[ind + VALSTART] = valstart;
			this.kvindex = kvnext;
		} catch (BufferTooSmallException e) {
			spillSingleRecord(key, value);
			return;
		}
	}

	private void serialize(Record record) throws IOException {
		((LocalRecord) record).serialize(this.outBuffer);
	}

	private void checkFull(int kvnext) throws IOException {
		this.spillLock.lock();
		try {
			boolean kvfull;
			do {
				if (this.sortSpillException != null) {
					throw new IOException(this.sortSpillException);
				}
				kvfull = kvnext == this.kvstart;
				final boolean kvsoftlimit = ((kvnext > this.kvend) ? kvnext - this.kvend > this.softRecordLimit
						: this.kvend - kvnext <= this.kvoffsets.length - this.softRecordLimit);
				if (this.kvstart == this.kvend && kvsoftlimit) {
					startSpill();
				}
				if (kvfull) {
					try {
						while (this.kvstart != this.kvend) {
							this.spillDone.await();
						}
					} catch (InterruptedException e) {
						throw new IOException(e);
					}
				}
			} while (kvfull);
		} finally {
			this.spillLock.unlock();
		}
	}

	@Override
	public int compare(int i, int j) {
		int ii = this.kvoffsets[i % this.kvoffsets.length];
		int ij = this.kvoffsets[j % this.kvoffsets.length];
		return this.comparator.compare(this.kvbuffer, this.kvindices[ii + KEYSTART], this.kvindices[ii
				+ VALSTART]
				- this.kvindices[ii + KEYSTART], this.kvbuffer, this.kvindices[ij + KEYSTART],
				this.kvindices[ij + VALSTART] - this.kvindices[ij + KEYSTART]);
	}

	@Override
	public void swap(int i, int j) {
		i %= this.kvoffsets.length;
		j %= this.kvoffsets.length;
		int tmp = this.kvoffsets[i];
		this.kvoffsets[i] = this.kvoffsets[j];
		this.kvoffsets[j] = tmp;
	}

	protected class MapDataOutput extends DataOutputStream {

		public MapDataOutput() {
			this(new MapOutputBuffer());
		}

		private MapDataOutput(OutputStream out) {
			super(out);
		}

		public int mark() {
			MapOutputCollector.this.bufmark = MapOutputCollector.this.bufindex;
			return MapOutputCollector.this.bufindex;
		}

		public synchronized void reset() throws IOException {
			int headbytelen = MapOutputCollector.this.bufvoid - MapOutputCollector.this.bufmark;
			MapOutputCollector.this.bufvoid = MapOutputCollector.this.bufmark;
			if (MapOutputCollector.this.bufindex + headbytelen < MapOutputCollector.this.bufstart) {
				System.arraycopy(MapOutputCollector.this.kvbuffer, 0, MapOutputCollector.this.kvbuffer,
						headbytelen, MapOutputCollector.this.bufindex);
				System.arraycopy(MapOutputCollector.this.kvbuffer, MapOutputCollector.this.bufvoid,
						MapOutputCollector.this.kvbuffer, 0, headbytelen);
				MapOutputCollector.this.bufindex += headbytelen;
			} else {
				byte[] keytmp = new byte[MapOutputCollector.this.bufindex];
				System.arraycopy(MapOutputCollector.this.kvbuffer, 0, keytmp, 0,
						MapOutputCollector.this.bufindex);
				MapOutputCollector.this.bufindex = 0;
				write(MapOutputCollector.this.kvbuffer, MapOutputCollector.this.bufmark, headbytelen);
				write(keytmp);
			}
		}

	}

	private class MapOutputBuffer extends OutputStream {
		private final byte[] scratch = new byte[1];

		@Override
		public synchronized void write(int v) throws IOException {
			this.scratch[0] = (byte) v;
			write(this.scratch, 0, 1);
		}

		@Override
		public synchronized void write(byte b[], int off, int len) throws IOException {
			boolean buffull = false;
			boolean wrap = false;
			MapOutputCollector.this.spillLock.lock();
			try {
				do {
					if (MapOutputCollector.this.sortSpillException != null) {
						throw (IOException) new IOException("Spill failed")
								.initCause(MapOutputCollector.this.sortSpillException);
					}

					if (MapOutputCollector.this.bufstart <= MapOutputCollector.this.bufend
							&& MapOutputCollector.this.bufend <= MapOutputCollector.this.bufindex) {
						buffull = MapOutputCollector.this.bufindex + len > MapOutputCollector.this.bufvoid;
						wrap = (MapOutputCollector.this.bufvoid - MapOutputCollector.this.bufindex)
								+ MapOutputCollector.this.bufstart > len;
					} else {
						wrap = false;
						buffull = MapOutputCollector.this.bufindex + len > MapOutputCollector.this.bufstart;
					}

					if (MapOutputCollector.this.kvstart == MapOutputCollector.this.kvend) {
						if (MapOutputCollector.this.kvend != MapOutputCollector.this.kvindex) {

							final boolean bufsoftlimit = (MapOutputCollector.this.bufindex > MapOutputCollector.this.bufend) ? MapOutputCollector.this.bufindex
									- MapOutputCollector.this.bufend > MapOutputCollector.this.softBufferLimit
									: MapOutputCollector.this.bufend - MapOutputCollector.this.bufindex < MapOutputCollector.this.bufvoid
											- MapOutputCollector.this.softBufferLimit;
							if (bufsoftlimit || (buffull && !wrap)) {
								startSpill();
							}
						} else if (buffull && !wrap) {
							final int size = ((MapOutputCollector.this.bufend <= MapOutputCollector.this.bufindex) ? MapOutputCollector.this.bufindex
									- MapOutputCollector.this.bufend
									: (MapOutputCollector.this.bufvoid - MapOutputCollector.this.bufend)
											+ MapOutputCollector.this.bufindex)
									+ len;
							MapOutputCollector.this.bufstart = MapOutputCollector.this.bufend = MapOutputCollector.this.bufindex = MapOutputCollector.this.bufmark = 0;
							MapOutputCollector.this.kvstart = MapOutputCollector.this.kvend = MapOutputCollector.this.kvindex = 0;
							MapOutputCollector.this.bufvoid = MapOutputCollector.this.kvbuffer.length;
							throw new BufferTooSmallException(size + " bytes");
						}
					}

					if (buffull && !wrap) {
						try {
							while (MapOutputCollector.this.kvstart != MapOutputCollector.this.kvend) {
								MapOutputCollector.this.spillDone.await();
							}
						} catch (InterruptedException e) {
							throw new IOException(e);
						}
					}
				} while (buffull && !wrap);
			} finally {
				MapOutputCollector.this.spillLock.unlock();
			}
			if (buffull) {
				final int gaplen = MapOutputCollector.this.bufvoid - MapOutputCollector.this.bufindex;
				System.arraycopy(b, off, MapOutputCollector.this.kvbuffer,
						MapOutputCollector.this.bufindex, gaplen);
				len -= gaplen;
				off += gaplen;
				MapOutputCollector.this.bufindex = 0;
			}
			System.arraycopy(b, off, MapOutputCollector.this.kvbuffer, MapOutputCollector.this.bufindex,
					len);
			MapOutputCollector.this.bufindex += len;
		}
	}

	public synchronized void flush() throws Exception {
		this.spillLock.lock();
		try {
			while (this.kvstart != this.kvend) {
				this.spillDone.await();
			}
			if (this.sortSpillException != null) {
				throw (IOException) new IOException("Spill failed").initCause(this.sortSpillException);
			}
			if (this.kvend != this.kvindex) {
				this.kvend = this.kvindex;
				this.bufend = this.bufmark;
				sortAndSpill();
			}
		} catch (InterruptedException e) {
			throw (IOException) new IOException("Buffer interrupted while waiting for the writer")
					.initCause(e);
		} finally {
			this.spillLock.unlock();
		}

		try {
			this.spillThread.interrupt();
			this.spillThread.join();
			this.spillThread = null;
		} catch (InterruptedException e) {
			throw e;
		}
		// kvbuffer = null;
		mergeParts();
	}

	public void close() {
	}

	protected class SpillThread extends Thread {
		@Override
		public void run() {
			MapOutputCollector.this.spillLock.lock();
			MapOutputCollector.this.spillThreadRunning = true;
			try {
				while (true) {
					MapOutputCollector.this.spillDone.signal();
					while (MapOutputCollector.this.kvstart == MapOutputCollector.this.kvend) {
						MapOutputCollector.this.spillReady.await();
					}
					try {
						MapOutputCollector.this.spillLock.unlock();
						sortAndSpill();
					} catch (Exception e) {
						MapOutputCollector.this.sortSpillException = e;
					} finally {
						MapOutputCollector.this.spillLock.lock();
						if (MapOutputCollector.this.bufend < MapOutputCollector.this.bufindex
								&& MapOutputCollector.this.bufindex < MapOutputCollector.this.bufstart) {
							MapOutputCollector.this.bufvoid = MapOutputCollector.this.kvbuffer.length;
						}
						MapOutputCollector.this.kvstart = MapOutputCollector.this.kvend;
						MapOutputCollector.this.bufstart = MapOutputCollector.this.bufend;
					}
				}
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			} finally {
				MapOutputCollector.this.spillLock.unlock();
				MapOutputCollector.this.spillThreadRunning = false;
			}
		}
	}

	private synchronized void startSpill() {
		this.kvend = this.kvindex;
		this.bufend = this.bufmark;
		this.spillReady.signal();
	}

	private void sortAndSpill() throws Exception {

		File file = this.fileHelper.getSpillFile(this.numSpills);

		final int endPosition = (this.kvend > this.kvstart) ? this.kvend : this.kvoffsets.length
				+ this.kvend;
		this.sorter.sort(MapOutputCollector.this, this.kvstart, endPosition);
		int spindex = this.kvstart;
		InMemValBytes value = new InMemValBytes();
		LocalRecordWriter writer = null;
		try {
			writer = new LocalRecordWriter(this.conf, file);
			if (!this.combine) {
				DataInputBuffer key = new DataInputBuffer();
				while (spindex < endPosition) {
					final int kvoff = this.kvoffsets[spindex % this.kvoffsets.length];
					getVBytesForOffset(kvoff, value);
					key.reset(this.kvbuffer, this.kvindices[kvoff + KEYSTART], (this.kvindices[kvoff
							+ VALSTART] - this.kvindices[kvoff + KEYSTART]));
					writer.append(key, value);
					++spindex;
				}
			} else {
				int spstart = spindex;
				spindex = endPosition;

				RawRecordIterator kvIter = new MapResultIterator(spstart, spindex);
				CombineDriver driver = new CombineDriver(this.conf, this.id, writer, kvIter);
				driver.run();
			}
		} finally {
			if (writer != null) {
				writer.close();
			}
		}
		++this.numSpills;
	}

	private void spillSingleRecord(final Record key, final Record value) throws IOException {
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
		++this.numSpills;
	}

	/**
	 * Given an offset, populate vbytes with the associated set of deserialized
	 * value bytes. Should only be called during a spill.
	 */
	private void getVBytesForOffset(int kvoff, InMemValBytes vbytes) {
		final int nextindex = (kvoff / ACCTSIZE == (this.kvend - 1 + this.kvoffsets.length)
				% this.kvoffsets.length) ? this.bufend : this.kvindices[(kvoff + ACCTSIZE + KEYSTART)
				% this.kvindices.length];
		int vallen = (nextindex >= this.kvindices[kvoff + VALSTART]) ? nextindex
				- this.kvindices[kvoff + VALSTART] : (this.bufvoid - this.kvindices[kvoff + VALSTART])
				+ nextindex;
		vbytes.reset(this.kvbuffer, this.kvindices[kvoff + VALSTART], vallen);
	}

	/**
	 * Inner class wrapping valuebytes, used for appendRaw.
	 */
	protected class InMemValBytes extends DataInputBuffer {
		private byte[] buffer;
		private int start;
		private int length;

		@Override
		public void reset(byte[] buffer, int start, int length) {
			this.buffer = buffer;
			this.start = start;
			this.length = length;

			if (start + length > MapOutputCollector.this.bufvoid) {
				this.buffer = new byte[this.length];
				final int taillen = MapOutputCollector.this.bufvoid - start;
				System.arraycopy(buffer, start, this.buffer, 0, taillen);
				System.arraycopy(buffer, 0, this.buffer, taillen, length - taillen);
				this.start = 0;
			}

			super.reset(this.buffer, this.start, this.length);
		}
	}

	protected class MapResultIterator implements RawRecordIterator {
		private final DataInputBuffer keybuf = new DataInputBuffer();
		private final InMemValBytes vbytes = new InMemValBytes();
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
			final int kvoff = MapOutputCollector.this.kvoffsets[this.current
					% MapOutputCollector.this.kvoffsets.length];
			this.keybuf.reset(MapOutputCollector.this.kvbuffer, MapOutputCollector.this.kvindices[kvoff
					+ KEYSTART], MapOutputCollector.this.kvindices[kvoff + VALSTART]
					- MapOutputCollector.this.kvindices[kvoff + KEYSTART]);
			return this.keybuf;
		}

		@Override
		public DataInputBuffer getValue() throws IOException {
			getVBytesForOffset(MapOutputCollector.this.kvoffsets[this.current
					% MapOutputCollector.this.kvoffsets.length], this.vbytes);
			return this.vbytes;
		}

		@Override
		public void close() {
		}
	}

	private void mergeParts() throws Exception {
		final File[] files = new File[this.numSpills];

		for (int i = 0; i < this.numSpills; i++) {
			files[i] = this.fileHelper.getSpillFile(i);
		}
		File finalOutputFile = this.fileHelper.getOutputFile();

		if (this.numSpills == 1) {
			files[0].renameTo(finalOutputFile);
			this.mapFiles.add(finalOutputFile);
			return;
		}

		if (this.numSpills == 0) {

			finalOutputFile.createNewFile();
			return;
		}
		List<RecordSegment> segments = new ArrayList<RecordSegment>(this.numSpills);
		for (int i = 0; i < this.numSpills; i++) {
			RecordSegment s = new RecordSegment(files[i], false);
			segments.add(i, s);
		}

		RawRecordIterator kvIter = RecordMerger.merge(segments, this.fileHelper.getTempDir(),
				this.conf.getSortFactor(), this.comparator);

		LocalRecordWriter writer = new LocalRecordWriter(this.conf, finalOutputFile);
		if (!this.combine || this.numSpills < this.minSpillsForCombine) {
			RecordMerger.writeFile(kvIter, writer);
		} else {
			// TODO
			CombineDriver driver = new CombineDriver(this.conf, this.id, writer, kvIter);
			driver.run();
		}
		this.mapFiles.add(finalOutputFile);
		writer.close();

		for (int i = 0; i < this.numSpills; i++) {
			files[i].delete();
		}
	}

	class BufferTooSmallException extends IOException {

		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;

		public BufferTooSmallException(String msg) {
			super(msg);
		}

	}

}
