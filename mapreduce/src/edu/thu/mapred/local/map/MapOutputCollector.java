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
import edu.thu.mapred.local.RawRecordComparator;
import edu.thu.mapred.local.RawRecordIterator;
import edu.thu.mapred.local.TaskFileHelper;
import edu.thu.mapred.local.io.DataInputBuffer;
import edu.thu.mapred.local.io.IndexSorter;
import edu.thu.mapred.local.io.LocalRecordWriter;
import edu.thu.mapred.local.io.RecordMerger;
import edu.thu.mapred.local.io.RecordMerger.RecordSegment;
import edu.thu.mapred.local.io.Sortable;

class MapOutputCollector implements Sortable {
	private final LocalJobConf conf;

	private RawRecordComparator comparator;

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

	private final IndexSorter sorter;
	private final ReentrantLock spillLock = new ReentrantLock();
	private final Condition spillDone = spillLock.newCondition();
	private final Condition spillReady = spillLock.newCondition();
	private final MapDataOutput outBuffer = new MapDataOutput();
	private volatile boolean spillThreadRunning = false;
	private final SpillThread spillThread = new SpillThread();

	private final TaskFileHelper fileHelper;
	private final TaskId id;
	private final List<File> mapFiles;

	public MapOutputCollector(LocalJobConf conf, TaskFileHelper fileHelper, TaskId id,
			List<File> mapFiles) throws Exception {
		this.conf = conf;
		this.fileHelper = fileHelper;
		sorter = new IndexSorter();

		int maxMemUsage = sortmb << 20;
		int recordCapacity = (int) (maxMemUsage * recper);
		recordCapacity -= recordCapacity % RECSIZE;
		kvbuffer = new byte[maxMemUsage - recordCapacity];
		bufvoid = kvbuffer.length;
		recordCapacity /= RECSIZE;
		kvoffsets = new int[recordCapacity];
		kvindices = new int[recordCapacity * ACCTSIZE];
		softBufferLimit = (int) (kvbuffer.length * spillper);
		softRecordLimit = (int) (kvoffsets.length * spillper);

		comparator = conf.getMapOutputKeyComparator();

		combine = conf.getCombinerClass() != null;
		this.id = id;
		this.mapFiles = mapFiles;

		spillThread.setDaemon(true);
		spillThread.setName("SpillThread");
		spillLock.lock();
		try {
			spillThread.start();
			while (!spillThreadRunning) {
				spillDone.await();
			}
		} catch (InterruptedException e) {
			throw e;
		} finally {
			spillLock.unlock();
		}
	}

	public synchronized void collect(Record key, Record value) throws IOException {
		final int kvnext = (kvindex + 1) % kvoffsets.length;

		checkFull(kvnext);
		try {

			int keystart = bufindex;
			serialize(key);
			if (bufindex < keystart) {

				outBuffer.reset();
				keystart = 0;
			}

			final int valstart = bufindex;
			serialize(value);
			outBuffer.mark();

			int ind = kvindex * ACCTSIZE;
			kvoffsets[kvindex] = ind;
			kvindices[ind + KEYSTART] = keystart;
			kvindices[ind + VALSTART] = valstart;
			kvindex = kvnext;
		} catch (BufferTooSmallException e) {
			spillSingleRecord(key, value);
			return;
		}
	}

	private void serialize(Record record) throws IOException {
		((LocalRecord) record).serialize(outBuffer);
	}

	private void checkFull(int kvnext) throws IOException {
		spillLock.lock();
		try {
			boolean kvfull;
			do {
				if (sortSpillException != null) {
					throw new IOException(sortSpillException);
				}
				kvfull = kvnext == kvstart;
				final boolean kvsoftlimit = ((kvnext > kvend) ? kvnext - kvend > softRecordLimit : kvend
						- kvnext <= kvoffsets.length - softRecordLimit);
				if (kvstart == kvend && kvsoftlimit) {
					startSpill();
				}
				if (kvfull) {
					try {
						while (kvstart != kvend) {
							spillDone.await();
						}
					} catch (InterruptedException e) {
						throw new IOException(e);
					}
				}
			} while (kvfull);
		} finally {
			spillLock.unlock();
		}
	}

	public int compare(int i, int j) {
		int ii = kvoffsets[i % kvoffsets.length];
		int ij = kvoffsets[j % kvoffsets.length];
		return comparator.compare(kvbuffer, kvindices[ii + KEYSTART], kvindices[ii + VALSTART]
				- kvindices[ii + KEYSTART], kvbuffer, kvindices[ij + KEYSTART], kvindices[ij + VALSTART]
				- kvindices[ij + KEYSTART]);
	}

	public void swap(int i, int j) {
		i %= kvoffsets.length;
		j %= kvoffsets.length;
		int tmp = kvoffsets[i];
		kvoffsets[i] = kvoffsets[j];
		kvoffsets[j] = tmp;
	}

	protected class MapDataOutput extends DataOutputStream {

		public MapDataOutput() {
			this(new MapOutputBuffer());
		}

		private MapDataOutput(OutputStream out) {
			super(out);
		}

		public int mark() {
			bufmark = bufindex;
			return bufindex;
		}

		public synchronized void reset() throws IOException {
			int headbytelen = bufvoid - bufmark;
			bufvoid = bufmark;
			if (bufindex + headbytelen < bufstart) {
				System.arraycopy(kvbuffer, 0, kvbuffer, headbytelen, bufindex);
				System.arraycopy(kvbuffer, bufvoid, kvbuffer, 0, headbytelen);
				bufindex += headbytelen;
			} else {
				byte[] keytmp = new byte[bufindex];
				System.arraycopy(kvbuffer, 0, keytmp, 0, bufindex);
				bufindex = 0;
				write(kvbuffer, bufmark, headbytelen);
				write(keytmp);
			}
		}

	}

	private class MapOutputBuffer extends OutputStream {
		private final byte[] scratch = new byte[1];

		@Override
		public synchronized void write(int v) throws IOException {
			scratch[0] = (byte) v;
			write(scratch, 0, 1);
		}

		@Override
		public synchronized void write(byte b[], int off, int len) throws IOException {
			boolean buffull = false;
			boolean wrap = false;
			spillLock.lock();
			try {
				do {
					if (sortSpillException != null) {
						throw (IOException) new IOException("Spill failed").initCause(sortSpillException);
					}

					if (bufstart <= bufend && bufend <= bufindex) {
						buffull = bufindex + len > bufvoid;
						wrap = (bufvoid - bufindex) + bufstart > len;
					} else {
						wrap = false;
						buffull = bufindex + len > bufstart;
					}

					if (kvstart == kvend) {
						if (kvend != kvindex) {

							final boolean bufsoftlimit = (bufindex > bufend) ? bufindex - bufend > softBufferLimit
									: bufend - bufindex < bufvoid - softBufferLimit;
							if (bufsoftlimit || (buffull && !wrap)) {
								startSpill();
							}
						} else if (buffull && !wrap) {
							final int size = ((bufend <= bufindex) ? bufindex - bufend : (bufvoid - bufend)
									+ bufindex)
									+ len;
							bufstart = bufend = bufindex = bufmark = 0;
							kvstart = kvend = kvindex = 0;
							bufvoid = kvbuffer.length;
							throw new BufferTooSmallException(size + " bytes");
						}
					}

					if (buffull && !wrap) {
						try {
							while (kvstart != kvend) {
								spillDone.await();
							}
						} catch (InterruptedException e) {
							throw new IOException(e);
						}
					}
				} while (buffull && !wrap);
			} finally {
				spillLock.unlock();
			}
			if (buffull) {
				final int gaplen = bufvoid - bufindex;
				System.arraycopy(b, off, kvbuffer, bufindex, gaplen);
				len -= gaplen;
				off += gaplen;
				bufindex = 0;
			}
			System.arraycopy(b, off, kvbuffer, bufindex, len);
			bufindex += len;
		}
	}

	public synchronized void flush() throws Exception {
		spillLock.lock();
		try {
			while (kvstart != kvend) {
				spillDone.await();
			}
			if (sortSpillException != null) {
				throw (IOException) new IOException("Spill failed").initCause(sortSpillException);
			}
			if (kvend != kvindex) {
				kvend = kvindex;
				bufend = bufmark;
				sortAndSpill();
			}
		} catch (InterruptedException e) {
			throw (IOException) new IOException("Buffer interrupted while waiting for the writer")
					.initCause(e);
		} finally {
			spillLock.unlock();
		}

		try {
			spillThread.interrupt();
			spillThread.join();
		} catch (InterruptedException e) {
			throw e;
		}
		kvbuffer = null;
		mergeParts();
	}

	public void close() {
	}

	protected class SpillThread extends Thread {
		@Override
		public void run() {
			spillLock.lock();
			spillThreadRunning = true;
			try {
				while (true) {
					spillDone.signal();
					while (kvstart == kvend) {
						spillReady.await();
					}
					try {
						spillLock.unlock();
						sortAndSpill();
					} catch (Exception e) {
						sortSpillException = e;
					} finally {
						spillLock.lock();
						if (bufend < bufindex && bufindex < bufstart) {
							bufvoid = kvbuffer.length;
						}
						kvstart = kvend;
						bufstart = bufend;
					}
				}
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			} finally {
				spillLock.unlock();
				spillThreadRunning = false;
			}
		}
	}

	private synchronized void startSpill() {
		kvend = kvindex;
		bufend = bufmark;
		spillReady.signal();
	}

	private void sortAndSpill() throws Exception {

		File file = fileHelper.getSpillFile(numSpills);

		final int endPosition = (kvend > kvstart) ? kvend : kvoffsets.length + kvend;
		sorter.sort(MapOutputCollector.this, kvstart, endPosition);
		int spindex = kvstart;
		InMemValBytes value = new InMemValBytes();
		LocalRecordWriter writer = null;
		try {
			writer = new LocalRecordWriter(conf, file);
			if (!combine) {
				DataInputBuffer key = new DataInputBuffer();
				while (spindex < endPosition) {
					final int kvoff = kvoffsets[spindex % kvoffsets.length];
					getVBytesForOffset(kvoff, value);
					key.reset(kvbuffer, kvindices[kvoff + KEYSTART],
							(kvindices[kvoff + VALSTART] - kvindices[kvoff + KEYSTART]));
					writer.append(key, value);
					++spindex;
				}
			} else {
				int spstart = spindex;
				spindex = endPosition;

				RawRecordIterator kvIter = new MapResultIterator(spstart, spindex);
				CombineDriver driver = new CombineDriver(conf, id, writer, kvIter);
				driver.run();
			}
		} finally {
			if (writer != null)
				writer.close();
		}
		++numSpills;
	}

	private void spillSingleRecord(final Record key, final Record value) throws IOException {
		File file = fileHelper.getSpillFile(numSpills);
		LocalRecordWriter writer = null;
		try {
			writer = new LocalRecordWriter(conf, file);
			writer.append(key, value);
		} finally {
			if (null != writer)
				writer.close();
		}
		++numSpills;
	}

	/**
	 * Given an offset, populate vbytes with the associated set of deserialized
	 * value bytes. Should only be called during a spill.
	 */
	private void getVBytesForOffset(int kvoff, InMemValBytes vbytes) {
		final int nextindex = (kvoff / ACCTSIZE == (kvend - 1 + kvoffsets.length) % kvoffsets.length) ? bufend
				: kvindices[(kvoff + ACCTSIZE + KEYSTART) % kvindices.length];
		int vallen = (nextindex >= kvindices[kvoff + VALSTART]) ? nextindex
				- kvindices[kvoff + VALSTART] : (bufvoid - kvindices[kvoff + VALSTART]) + nextindex;
		vbytes.reset(kvbuffer, kvindices[kvoff + VALSTART], vallen);
	}

	/**
	 * Inner class wrapping valuebytes, used for appendRaw.
	 */
	protected class InMemValBytes extends DataInputBuffer {
		private byte[] buffer;
		private int start;
		private int length;

		public void reset(byte[] buffer, int start, int length) {
			this.buffer = buffer;
			this.start = start;
			this.length = length;

			if (start + length > bufvoid) {
				this.buffer = new byte[this.length];
				final int taillen = bufvoid - start;
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
			current = start - 1;
		}

		public boolean next() throws IOException {
			return ++current < end;
		}

		public DataInputBuffer getKey() throws IOException {
			final int kvoff = kvoffsets[current % kvoffsets.length];
			keybuf.reset(kvbuffer, kvindices[kvoff + KEYSTART], kvindices[kvoff + VALSTART]
					- kvindices[kvoff + KEYSTART]);
			return keybuf;
		}

		public DataInputBuffer getValue() throws IOException {
			getVBytesForOffset(kvoffsets[current % kvoffsets.length], vbytes);
			return vbytes;
		}

		public void close() {
		}
	}

	private void mergeParts() throws Exception {
		final File[] files = new File[numSpills];

		for (int i = 0; i < numSpills; i++) {
			files[i] = fileHelper.getSpillFile(i);
		}
		if (numSpills == 1) {
			files[0].renameTo(new File(files[0].getParentFile(), "file.out"));
			return;
		}
		File finalOutputFile = fileHelper.getOutputFile();

		if (numSpills == 0) {

			finalOutputFile.createNewFile();
			return;
		}
		List<RecordSegment> segments = new ArrayList<RecordSegment>(numSpills);
		for (int i = 0; i < numSpills; i++) {
			RecordSegment s = new RecordSegment(files[i], false);
			segments.add(i, s);
		}

		RawRecordIterator kvIter = RecordMerger.merge(segments, fileHelper.getTempDir(),
				conf.getSortFactor(), comparator);

		LocalRecordWriter writer = new LocalRecordWriter(conf, finalOutputFile);
		if (!combine || numSpills < minSpillsForCombine) {
			RecordMerger.writeFile(kvIter, writer);
		} else {
			// TODO
			CombineDriver driver = new CombineDriver(conf, id, writer, kvIter);
			driver.run();
		}
		mapFiles.add(finalOutputFile);
		writer.close();

		for (int i = 0; i < numSpills; i++) {
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
