package edu.thu.mapred.local.io;

import java.io.BufferedInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

import edu.thu.mapred.local.LocalJobConf;
import edu.thu.mapred.local.util.IOUtil;

public class LocalRecordReader {
	//buffer 512k
	private static final int MAX_HEADER_SIZE = 2 * 9;

	protected final InputStream in;
	protected boolean eof = false;
	protected byte[] buffer = null;
	protected int bufferSize;
	protected DataInputBuffer dataIn = new DataInputBuffer();

	public LocalRecordReader(LocalJobConf conf, File file) throws IOException {
		this.in = new BufferedInputStream(new FileInputStream(file));
		this.bufferSize = conf.getReadBufferSize();
	}

	private int readData(byte[] buf, int off, int len) throws IOException {
		int bytesRead = 0;
		while (bytesRead < len) {
			int n = this.in.read(buf, off + bytesRead, len - bytesRead);
			if (n < 0) {
				return bytesRead;
			}
			bytesRead += n;
		}
		return len;
	}

	void readNextBlock(int minSize) throws IOException {
		if (this.buffer == null) {
			this.buffer = new byte[this.bufferSize];
			this.dataIn.reset(this.buffer, 0, 0);
		}
		this.buffer = adjustData(this.buffer, (this.bufferSize < minSize) ? new byte[minSize << 1]
				: this.buffer);
		this.bufferSize = this.buffer.length;
	}

	private byte[] adjustData(byte[] source, byte[] destination) throws IOException {
		int bytesRemaining = this.dataIn.getLength() - this.dataIn.getPosition();
		if (bytesRemaining > 0) {
			System.arraycopy(source, this.dataIn.getPosition(), destination, 0, bytesRemaining);
		}

		int n = readData(destination, bytesRemaining, (destination.length - bytesRemaining));
		this.dataIn.reset(destination, 0, (bytesRemaining + n));

		return destination;
	}

	public boolean next(DataInputBuffer key, DataInputBuffer value) throws IOException {
		if (this.eof) {
			throw new EOFException();
		}

		if ((this.dataIn.getLength() - this.dataIn.getPosition()) < MAX_HEADER_SIZE) {
			readNextBlock(MAX_HEADER_SIZE);
		}

		int keyLength = IOUtil.readVInt(this.dataIn);
		int valueLength = IOUtil.readVInt(this.dataIn);

		if (keyLength == -1 && valueLength == -1) {
			this.eof = true;
			return false;
		}

		final int recordLength = keyLength + valueLength;

		int pos = this.dataIn.getPosition();

		if ((this.dataIn.getLength() - pos) < recordLength) {
			readNextBlock(recordLength);
		}

		pos = this.dataIn.getPosition();
		byte[] data = this.dataIn.getData();
		key.reset(data, pos, keyLength);
		value.reset(data, (pos + keyLength), valueLength);

		this.dataIn.skip(recordLength);

		return true;
	}

	public void close() throws IOException {

		this.in.close();

		this.dataIn = null;
		this.buffer = null;

	}

}
