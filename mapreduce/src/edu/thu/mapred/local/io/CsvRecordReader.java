package edu.thu.mapred.local.io;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import com.aliyun.odps.Column;
import com.aliyun.odps.OdpsType;
import com.aliyun.odps.data.Record;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;

import edu.thu.mapred.local.LocalRecord;

public class CsvRecordReader {

	private LocalRecord record;
	private CsvParser reader;
	private String[] row;
	private boolean eof = false;

	public CsvRecordReader(File file) throws IOException {
		this.reader = new CsvParser(new CsvParserSettings());
		this.reader.beginParsing(new FileReader(file));
	}

	public Record read() throws IOException {
		if (eof) {
			return null;
		}
		row = this.reader.parseNext();
		if (row == null) {
			eof = true;
			return null;
		}
		if (this.record == null) {
			int columns = row.length;
			Column[] schema = new Column[columns];
			for (int i = 0; i < columns; i++) {
				schema[i] = new Column("col" + i, OdpsType.STRING);
			}
			this.record = new LocalRecord(schema);
		}
		this.record.fastSet(this.row);
		return this.record;
	}

	public void close() {
		this.reader.stopParsing();
	}
}
