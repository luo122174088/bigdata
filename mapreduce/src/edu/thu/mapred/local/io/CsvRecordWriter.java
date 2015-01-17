package edu.thu.mapred.local.io;

import java.io.File;
import java.io.IOException;

import com.aliyun.odps.data.Record;
import com.csvreader.CsvWriter;

public class CsvRecordWriter {

	private CsvWriter writer;

	public CsvRecordWriter(File file) throws IOException {
		this.writer = new CsvWriter(file.getAbsolutePath());
	}

	public void write(Record record) throws IOException {
		int count = record.getColumnCount();
		for (int i = 0; i < count; i++) {
			this.writer.write(record.get(i).toString());
		}
		this.writer.endRecord();
	}

	public void close() throws IOException {
		this.writer.close();
	}

}
