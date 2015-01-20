package edu.thu.mapred.local.io;

import java.io.IOException;

import edu.thu.mapred.local.util.IOUtil;

public class MemoryRecordReader extends LocalRecordReader {

	public MemoryRecordReader(byte[] buffer, int start, int length) throws IOException {
		this.buffer = buffer;
		this.dataIn.reset(buffer, start, length);
	}

	@Override
	public boolean next(DataInputBuffer key, DataInputBuffer value) throws IOException {
		if (this.eof) {
			return false;
		}
		int keyLength = IOUtil.readVInt(dataIn);
		int valueLength = IOUtil.readVInt(dataIn);

		if (keyLength == -1 && valueLength == -1) {
			this.eof = true;
			return false;
		}
		int pos = this.dataIn.getPosition();
		int recordLength = keyLength + valueLength;
		key.reset(buffer, pos, keyLength);
		value.reset(buffer, (pos + keyLength), valueLength);
		this.dataIn.skip(recordLength);
		return true;
	}
}
