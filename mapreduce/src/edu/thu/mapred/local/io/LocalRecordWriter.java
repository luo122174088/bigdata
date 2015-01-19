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
		this.out = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(file),
				conf.getWriteBufferSize()));
	}

	public void close() throws IOException {
		IOUtil.writeVInt(out, -1);
		IOUtil.writeVInt(out, -1);

		this.out.flush();
		this.out.close();

	}

	public void append(Record key, Record value) throws IOException {
		((LocalRecord) key).serialize(this.buffer);
		int keyLength = this.buffer.getLength();
		((LocalRecord) value).serialize(this.buffer);
		int valueLength = this.buffer.getLength() - keyLength;

		IOUtil.writeVInt(out, keyLength);
		IOUtil.writeVInt(out, valueLength);

		this.out.write(this.buffer.getData(), 0, this.buffer.getLength()); // data

		this.buffer.reset();
	}

	public void append(DataInputBuffer key, DataInputBuffer value) throws IOException {
		int keyLength = key.getLength() - key.getPosition();

		int valueLength = value.getLength() - value.getPosition();

		IOUtil.writeVInt(out, keyLength);
		IOUtil.writeVInt(out, valueLength);

		this.out.write(key.getData(), key.getPosition(), keyLength);
		this.out.write(value.getData(), value.getPosition(), valueLength);
	}
}
