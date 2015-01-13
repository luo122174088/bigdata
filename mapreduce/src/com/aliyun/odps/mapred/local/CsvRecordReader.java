package com.aliyun.odps.mapred.local;

import java.io.FileNotFoundException;
import java.io.IOException;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.local.conf.LocalJobConf;
import com.csvreader.CsvReader;

public class CsvRecordReader {

	private String path;
	private LineReader reader;
	private Record record;

	public CsvRecordReader(String path, LocalJobConf conf) throws IOException {

	}

	public Record read() {

		return null;
	}

}
