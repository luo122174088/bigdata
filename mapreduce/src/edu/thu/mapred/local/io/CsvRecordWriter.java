package edu.thu.mapred.local.io;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import com.aliyun.odps.data.Record;

import edu.thu.mapred.local.LocalJobConf;

public class CsvRecordWriter {

	private BufferedWriter writer;

	public CsvRecordWriter(LocalJobConf conf, File file) throws IOException {
		this.writer = new BufferedWriter(new FileWriter(file), conf.getWriteBufferSize());
	}

	public void write(Record record) throws IOException {
		int count = record.getColumnCount();
		for (int i = 0; i < count; i++) {
			this.writer.write(record.get(i).toString());
			if (i < count - 1) {
				this.writer.write(',');
			}
		}
		this.writer.write('\n');
	}

	public void close() throws IOException {
		this.writer.close();
	}

}
