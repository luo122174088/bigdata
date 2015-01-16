package edu.thu.mapred.local.map;

import java.io.File;
import java.io.IOException;
import java.util.List;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.TableInfo;
import com.aliyun.odps.mapred.Mapper;
import com.aliyun.odps.mapred.Mapper.TaskContext;
import com.aliyun.odps.mapred.TaskId;

import edu.thu.mapred.local.BaseDriver;
import edu.thu.mapred.local.LocalJobConf;
import edu.thu.mapred.local.LocalTaskContext;
import edu.thu.mapred.local.TaskFileHelper;
import edu.thu.mapred.local.io.CsvRecordReader;

public class MapDriver extends BaseDriver {

	protected InputSplit split;

	public MapDriver(LocalJobConf conf, TaskId id, InputSplit split, List<File> mapFiles) {
		super(conf, id, mapFiles);
		this.split = split;

	}

	@Override
	public void run() throws Exception {
		Class<? extends Mapper> mapperClass = conf.getMapperClass();
		Mapper mapper = mapperClass.newInstance();

		CsvRecordReader reader = new CsvRecordReader(split.file);

		MapOutputCollector collector = new MapOutputCollector(conf, new TaskFileHelper(this), id,
				mapFiles);
		MapTaskContext context = new MapTaskContext(conf, id, collector);
		mapper.setup(context);

		Record record = null;
		try {
			while ((record = reader.read()) != null) {
				mapper.map(0, record, context);
			}
		} finally {
			reader.close();
			collector.flush();
			collector.close();
			mapper.cleanup(context);
		}

	}

	class MapTaskContext extends LocalTaskContext implements TaskContext {

		private MapOutputCollector collector;

		public MapTaskContext(LocalJobConf conf, TaskId id, MapOutputCollector collector) {
			super(conf, id);
			this.collector = collector;
		}

		@Override
		public void write(Record key, Record value) throws IOException {
			collector.collect(key, value);
		}

		@Override
		public long getCurrentRecordNum() {
			throw new UnsupportedOperationException("Unimplemented");
		}

		@Override
		public Record getCurrentRecord() {
			throw new UnsupportedOperationException("Unimplemented");
		}

		@Override
		public boolean nextRecord() {
			throw new UnsupportedOperationException("Unimplemented");
		}

		@Override
		public TableInfo getInputTableInfo() {
			throw new UnsupportedOperationException("Unimplemented");
		}

	}
}
