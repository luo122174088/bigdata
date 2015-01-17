package edu.thu.mapred.local.io;

import java.io.File;
import java.io.IOException;

import com.aliyun.odps.Column;
import com.aliyun.odps.OdpsType;
import com.aliyun.odps.data.Record;
import com.csvreader.CsvReader;

import edu.thu.mapred.local.LocalRecord;

public class CsvRecordReader {

	private LocalRecord record;
	private CsvReader reader;

	public CsvRecordReader(File file) throws IOException {
		this.reader = new CsvReader(file.getAbsolutePath());
	}

	public Record read() throws IOException {
		if (!this.reader.readRecord()) {
			return null;
		}
		if (this.record == null) {
			int columns = this.reader.getColumnCount();
			Column[] schema = new Column[columns];
			for (int i = 0; i < columns; i++) {
				schema[i] = new Column("col" + i, OdpsType.STRING);
			}
			this.record = new LocalRecord(schema);
		}
		this.record.fastSet(this.reader.getValues());
		return this.record;
	}

	public void close() {
		this.reader.close();
	}
}
