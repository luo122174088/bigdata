package edu.thu.mapred.local.io;

import java.io.BufferedInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

import edu.thu.mapred.local.util.IOUtil;

public class LocalRecordReader {
	private static final int DEFAULT_BUFFER_SIZE = 512 * 1024;
	private static final int MAX_VINT_SIZE = 9;

	protected final InputStream in;
	protected boolean eof = false;
	protected byte[] buffer = null;
	protected int bufferSize = DEFAULT_BUFFER_SIZE;
	protected DataInputBuffer dataIn = new DataInputBuffer();

	public LocalRecordReader(File file) throws IOException {
		this.in = new BufferedInputStream(new FileInputStream(file));
	}

	private int readData(byte[] buf, int off, int len) throws IOException {
		int bytesRead = 0;
		while (bytesRead < len) {
			int n = in.read(buf, off + bytesRead, len - bytesRead);
			if (n < 0) {
				return bytesRead;
			}
			bytesRead += n;
		}
		return len;
	}

	void readNextBlock(int minSize) throws IOException {
		if (buffer == null) {
			buffer = new byte[bufferSize];
			dataIn.reset(buffer, 0, 0);
		}
		buffer = rejigData(buffer, (bufferSize < minSize) ? new byte[minSize << 1] : buffer);
		bufferSize = buffer.length;
	}

	private byte[] rejigData(byte[] source, byte[] destination) throws IOException {
		// Copy remaining data into the destination array
		int bytesRemaining = dataIn.getLength() - dataIn.getPosition();
		if (bytesRemaining > 0) {
			System.arraycopy(source, dataIn.getPosition(), destination, 0, bytesRemaining);
		}

		int n = readData(destination, bytesRemaining, (destination.length - bytesRemaining));
		dataIn.reset(destination, 0, (bytesRemaining + n));

		return destination;
	}

	public boolean next(DataInputBuffer key, DataInputBuffer value) throws IOException {
		if (eof) {
			throw new EOFException();
		}

		if ((dataIn.getLength() - dataIn.getPosition()) < 2 * MAX_VINT_SIZE) {
			readNextBlock(2 * MAX_VINT_SIZE);
		}

		// Read key and value lengths
		int keyLength = IOUtil.readVInt(dataIn);
		int valueLength = IOUtil.readVInt(dataIn);
		int pos = dataIn.getPosition();

		if (keyLength == 0 && valueLength == 0) {
			eof = true;
			return false;
		}

		final int recordLength = keyLength + valueLength;

		if ((dataIn.getLength() - pos) < recordLength) {
			readNextBlock(recordLength);
		}

		pos = dataIn.getPosition();
		byte[] data = dataIn.getData();
		key.reset(data, pos, keyLength);
		value.reset(data, (pos + keyLength), valueLength);

		dataIn.skip(recordLength);

		return true;
	}

	public void close() throws IOException {

		in.close();

		dataIn = null;
		buffer = null;

	}

}
