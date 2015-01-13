package com.aliyun.odps.mapred.local;

import java.io.BufferedInputStream;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.Iterator;
import java.util.List;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.Reducer;
import com.aliyun.odps.mapred.Reducer.TaskContext;
import com.aliyun.odps.mapred.TaskId;
import com.aliyun.odps.mapred.local.conf.LocalJobConf;

public class ReduceDriver extends BaseDriver {

	LocalJobConf conf;

	List<File> mapFiles;

	TaskId id;

	public ReduceDriver(LocalJobConf conf, TaskId id, List<File> mapFiles) {
		this.conf = conf;
		this.mapFiles = mapFiles;
		this.id = id;
	}

	@Override
	public void run() throws Exception {
		Class<? extends Reducer> reducerClass = conf.getReducerClass();

		Reducer reducer = reducerClass.newInstance();

		File file = new File(conf.getReduceDir(), id.toString());
		CsvWriter writer = new CsvWriter(file);

		TaskContext context = new ReduceTaskContext(conf, id, writer);

		reducer.setup(context);

		Record key = new LocalRecord(conf.getMapOutputKeySchema());
		Record value = new LocalRecord(conf.getMapOutputValueSchema());
		for (File mapFile : mapFiles) {
			DataInputStream input = new DataInputStream(new BufferedInputStream(
					new FileInputStream(mapFile)));

			String word = input.readUTF();
			while (true) {
				ValueIterator iterator = new ValueIterator(word, input, value);
				key.set(0, word);
				reducer.reduce(key, iterator, context);
				word = iterator.nextKey;
				if (word == null) {
					break;
				}
			}
		}

		reducer.cleanup(context);

		writer.close();

	}

	class ValueIterator implements Iterator<Record> {
		String key;
		DataInputStream input;
		String nextKey;
		Record record;

		public ValueIterator(String key, DataInputStream input, Record record) {
			this.key = key;
			this.input = input;
			this.record = record;
			this.nextKey = key;
		}

		@Override
		public boolean hasNext() {
			try {
				if (nextKey == null) {
					nextKey = input.readUTF();
				}
				return nextKey.equals(key);
			} catch (IOException e) {
				return false;
			}
		}

		@Override
		public Record next() {
			long count;
			try {
				count = input.readLong();
				nextKey = null;
			} catch (IOException e) {
				return null;
			}
			record.set(0, count);
			return record;
		}

	}

	class ReduceTaskContext extends LocalTaskContext implements TaskContext {

		private CsvWriter writer;

		public ReduceTaskContext(LocalJobConf conf, TaskId id, CsvWriter writer) {
			super(conf, id);
			this.writer = writer;
		}

		@Override
		public void write(Record record) throws IOException {
			writer.write(record);
		}

		@Override
		public boolean nextKeyValue() {
			throw new UnsupportedOperationException("Unimplemented");
		}

		@Override
		public Record getCurrentKey() {
			throw new UnsupportedOperationException("Unimplemented");
		}

		@Override
		public Iterator<Record> getValues() {
			throw new UnsupportedOperationException("Unimplemented");
		}
	}

}
