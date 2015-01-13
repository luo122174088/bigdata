package com.aliyun.odps.mapred.local;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import com.aliyun.odps.data.Record;

public class CsvWriter {

	File file;

	private BufferedWriter writer;

	public CsvWriter(File file) throws IOException {
		writer = new BufferedWriter(new FileWriter(file));
	}

	public void write(Record record) throws IOException {
		int count = record.getColumnCount();
		for (int i = 0; i < count; i++) {
			writer.write(record.get(i).toString());
			if (i < count - 1) {
				writer.write(',');
			}
		}

		writer.write('\n');
	}

	public void close() throws IOException {
		writer.close();
	}

}
