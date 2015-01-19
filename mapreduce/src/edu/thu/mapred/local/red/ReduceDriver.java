package edu.thu.mapred.local.red;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.Reducer;
import com.aliyun.odps.mapred.Reducer.TaskContext;
import com.aliyun.odps.mapred.TaskId;

import edu.thu.mapred.local.BaseDriver;
import edu.thu.mapred.local.LocalJobConf;
import edu.thu.mapred.local.LocalTaskContext;
import edu.thu.mapred.local.io.CsvRecordWriter;
import edu.thu.mapred.local.io.LocalRecordIterator;
import edu.thu.mapred.local.io.RawRecordIterator;
import edu.thu.mapred.local.io.RecordMerger;
import edu.thu.mapred.local.io.RecordMerger.RecordSegment;

public class ReduceDriver extends BaseDriver {

	public ReduceDriver(LocalJobConf conf, TaskId id, List<File> mapFiles) throws Exception {
		super(conf, mapFiles);
		this.init(id);
	}

	@Override
	public void runInternal() throws Exception {
		logger.info("Reduce task {} starts.", this.id);
		long start = System.currentTimeMillis();

		List<RecordSegment> segments = new ArrayList<>(this.mapFiles.size());
		for (File file : this.mapFiles) {
			segments.add(new RecordSegment(conf, file, false));
		}
		RawRecordIterator in = RecordMerger.merge(conf, segments, this.fileHelper.getTempDir(),
				this.conf.getSortFactor(), this.conf.getMapOutputKeyComparator());

		Class<? extends Reducer> reducerClass = this.conf.getReducerClass();
		Reducer reducer = reducerClass.newInstance();

		File outputFile = new File(getTaskDir(), "part-" + this.id.getInstId());
		CsvRecordWriter writer = new CsvRecordWriter(conf, outputFile);
		TaskContext context = new ReduceTaskContext(this.conf, this.id, writer);
		LocalRecordIterator iterator = new LocalRecordIterator(in,
				this.conf.getMapOutputKeyComparator(), this.conf.getMapOutputKeySchema(),
				this.conf.getMapOutputValueSchema());

		reducer.setup(context);
		try {
			while (iterator.more()) {
				reducer.reduce(iterator.getKey(), iterator, context);
				iterator.nextKey();
			}
		} finally {
			writer.close();
			reducer.cleanup(context);
			long end = System.currentTimeMillis();
			logger.info("Reduce task {} ends in {}ms", this.id, (end - start));
		}
	}

	@Override
	public String getTaskDir() {
		return this.conf.getReduceDir() + this.id.toString() + "/";
	}

	class ReduceTaskContext extends LocalTaskContext implements TaskContext {

		private CsvRecordWriter writer;

		public ReduceTaskContext(LocalJobConf conf, TaskId id, CsvRecordWriter writer) {
			super(conf, id);
			this.writer = writer;
		}

		@Override
		public void write(Record record) throws IOException {
			this.writer.write(record);
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
