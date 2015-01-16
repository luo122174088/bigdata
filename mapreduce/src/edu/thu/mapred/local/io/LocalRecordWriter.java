package edu.thu.mapred.local.io;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

import com.aliyun.odps.data.Record;

import edu.thu.mapred.local.LocalJobConf;
import edu.thu.mapred.local.LocalRecord;
import edu.thu.mapred.local.util.IOUtil;

public class LocalRecordWriter {
	DataOutputStream out;
	DataOutputBuffer buffer = new DataOutputBuffer();

	public LocalRecordWriter(LocalJobConf conf, File file) throws IOException {
		out = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(file)));
	}

	public void close() throws IOException {
		out.writeInt(-1);
		out.writeInt(-1);

		out.flush();
		out.close();

	}

	public void append(Record key, Record value) throws IOException {
		// Append the 'key'
		((LocalRecord) key).serialize(buffer);
		int keyLength = buffer.getLength();
		((LocalRecord) value).serialize(buffer);
		int valueLength = buffer.getLength() - keyLength;

		IOUtil.writeVInt(out, keyLength);
		IOUtil.writeVInt(out, valueLength);

		out.write(buffer.getData(), 0, buffer.getLength()); // data

		buffer.reset();
	}

	public void append(DataInputBuffer key, DataInputBuffer value) throws IOException {
		int keyLength = key.getLength() - key.getPosition();

		int valueLength = value.getLength() - value.getPosition();
		IOUtil.writeVInt(out, keyLength);
		IOUtil.writeVInt(out, valueLength);

		out.write(key.getData(), key.getPosition(), keyLength);
		out.write(value.getData(), value.getPosition(), valueLength);
	}
}
