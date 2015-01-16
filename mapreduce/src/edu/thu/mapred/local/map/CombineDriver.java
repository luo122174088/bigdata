package edu.thu.mapred.local.map;

import java.io.IOException;
import java.util.Iterator;

import com.aliyun.odps.Column;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.Reducer;
import com.aliyun.odps.mapred.Reducer.TaskContext;
import com.aliyun.odps.mapred.TaskId;

import edu.thu.mapred.local.BaseDriver;
import edu.thu.mapred.local.LocalJobConf;
import edu.thu.mapred.local.LocalTaskContext;
import edu.thu.mapred.local.RawRecordComparator;
import edu.thu.mapred.local.RawRecordIterator;
import edu.thu.mapred.local.io.LocalRecordIterator;
import edu.thu.mapred.local.io.LocalRecordWriter;

public class CombineDriver extends BaseDriver {

	private RawRecordIterator in;
	private LocalRecordWriter writer;
	private Column[] keySchema;
	private Column[] valueSchema;

	private RawRecordComparator comparator;

	public CombineDriver(LocalJobConf conf, TaskId id, LocalRecordWriter writer,
			RawRecordIterator iterator) {
		super(conf, id);
		this.writer = writer;
		this.in = iterator;
		this.keySchema = conf.getMapOutputKeySchema();
		this.valueSchema = conf.getMapOutputValueSchema();

	}

	@Override
	public void run() throws Exception {
		Class<? extends Reducer> combinerClass = conf.getCombinerClass();

		Reducer combiner = combinerClass.newInstance();
		TaskContext context = new CombineTaskContext();
		combiner.setup(context);
		LocalRecordIterator iterator = new LocalRecordIterator(in, comparator, keySchema, valueSchema);
		try {
			while (iterator.more()) {
				combiner.reduce(iterator.getKey(), iterator, context);
				iterator.nextKey();
			}
		} finally {
			combiner.cleanup(context);
		}
	}

	class CombineTaskContext extends LocalTaskContext implements TaskContext {

		public CombineTaskContext() {
			super(conf, id);
		}

		@Override
		public void write(Record key, Record value) throws IOException {
			writer.append(key, value);
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
