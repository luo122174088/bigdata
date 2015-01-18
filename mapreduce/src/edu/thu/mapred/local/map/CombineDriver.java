package edu.thu.mapred.local.map;

import java.io.IOException;
import java.util.Iterator;

import com.aliyun.odps.Column;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.Reducer;
import com.aliyun.odps.mapred.Reducer.TaskContext;
import com.aliyun.odps.mapred.ReducerBase;
import com.aliyun.odps.mapred.TaskId;

import edu.thu.mapred.local.BaseDriver;
import edu.thu.mapred.local.LocalJobConf;
import edu.thu.mapred.local.LocalTaskContext;
import edu.thu.mapred.local.io.LocalRecordIterator;
import edu.thu.mapred.local.io.LocalRecordWriter;
import edu.thu.mapred.local.io.RawRecordIterator;
import edu.thu.mapred.local.util.RecordComparator;

public class CombineDriver extends BaseDriver {

	private RawRecordIterator in;
	private LocalRecordWriter writer;
	private Column[] keySchema;
	private Column[] valueSchema;

	private RecordComparator comparator;

	public CombineDriver(LocalJobConf conf, TaskId id, LocalRecordWriter writer,
			RawRecordIterator iterator) {
		super(conf);
		this.writer = writer;
		this.in = iterator;
		this.keySchema = conf.getMapOutputKeySchema();
		this.valueSchema = conf.getMapOutputValueSchema();
		this.comparator = conf.getMapOutputKeyComparator();
	}

	@Override
	public void runInternal() throws Exception {
		Class<? extends Reducer> combinerClass = this.conf.getCombinerClass();
		if (combinerClass == null) {
			combinerClass = DummyCombiner.class;
		}
		Reducer combiner = combinerClass.newInstance();
		TaskContext context = new CombineTaskContext();
		combiner.setup(context);
		LocalRecordIterator iterator = new LocalRecordIterator(this.in, this.comparator,
				this.keySchema, this.valueSchema);
		try {
			while (iterator.more()) {
				combiner.reduce(iterator.getKey(), iterator, context);
				iterator.nextKey();
			}
		} finally {
			combiner.cleanup(context);
		}
	}

	private static class DummyCombiner extends ReducerBase {
		@Override
		public void reduce(Record key, Iterator<Record> values, TaskContext context) throws IOException {
			while (values.hasNext()) {
				context.write(key, values.next());
			}
		}
	}

	@Override
	public String getTaskDir() {
		return this.conf.getMapDir() + this.id.toString() + "/";
	}

	class CombineTaskContext extends LocalTaskContext implements TaskContext {

		public CombineTaskContext() {
			super(CombineDriver.this.conf, CombineDriver.this.id);
		}

		@Override
		public void write(Record key, Record value) throws IOException {
			CombineDriver.this.writer.append(key, value);
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
