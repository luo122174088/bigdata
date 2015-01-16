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
import edu.thu.mapred.local.RawRecordIterator;
import edu.thu.mapred.local.io.CsvRecordWriter;
import edu.thu.mapred.local.io.LocalRecordIterator;
import edu.thu.mapred.local.io.RecordMerger;
import edu.thu.mapred.local.io.RecordMerger.RecordSegment;

public class ReduceDriver extends BaseDriver {

	public ReduceDriver(LocalJobConf conf, TaskId id, List<File> mapFiles) {
		super(conf, id, mapFiles);
	}

	@Override
	public void run() throws Exception {
		List<RecordSegment> segments = new ArrayList<>(mapFiles.size());
		for (File file : mapFiles) {
			segments.add(new RecordSegment(file, false));
		}
		RawRecordIterator in = RecordMerger.merge(segments, fileHelper.getTempDir(),
				conf.getSortFactor(), conf.getMapOutputKeyComparator());

		Class<? extends Reducer> reducerClass = conf.getReducerClass();
		Reducer reducer = reducerClass.newInstance();

		File outputFile = new File(conf.getReduceDir(), id.toString());
		CsvRecordWriter writer = new CsvRecordWriter(outputFile);
		TaskContext context = new ReduceTaskContext(conf, id, writer);
		LocalRecordIterator iterator = new LocalRecordIterator(in, conf.getMapOutputKeyComparator(),
				conf.getMapOutputKeySchema(), conf.getMapOutputValueSchema());

		reducer.setup(context);
		try {
			while (iterator.more()) {
				while (iterator.more()) {
					reducer.reduce(iterator.getKey(), iterator, context);
					iterator.nextKey();
				}
			}
		} finally {
			writer.close();
			reducer.cleanup(context);
		}

	}

	class ReduceTaskContext extends LocalTaskContext implements TaskContext {

		private CsvRecordWriter writer;

		public ReduceTaskContext(LocalJobConf conf, TaskId id, CsvRecordWriter writer) {
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
