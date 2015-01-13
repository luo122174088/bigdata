package com.aliyun.odps.mapred.local;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.List;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.TableInfo;
import com.aliyun.odps.io.Text;
import com.aliyun.odps.mapred.Mapper;
import com.aliyun.odps.mapred.Mapper.TaskContext;
import com.aliyun.odps.mapred.TaskId;
import com.aliyun.odps.mapred.local.conf.LocalJobConf;

public class MapDriver extends BaseDriver {

	LocalJobConf conf;
	InputSplit split;
	TaskId id;
	List<File> mapFiles;

	public MapDriver(LocalJobConf conf, TaskId id, InputSplit split,
			List<File> mapFiles) {
		this.conf = conf;
		this.split = split;
		this.id = id;
		this.mapFiles = mapFiles;
	}

	@Override
	public void run() throws Exception {
		Class<? extends Mapper> mapperClass = conf.getMapperClass();

		Mapper mapper = mapperClass.newInstance();
		LineReader reader = new LineReader(new FileInputStream(split.file));
		OutputCollector collector = new OutputCollector(conf, id, mapFiles);
		MapTaskContext context = new MapTaskContext(conf, id, collector);
		Record record = new LocalRecord(1);
		Text text = new Text();
		mapper.setup(context);
		while (reader.readLine(text) > 0) {
			record.set(0, text.toString());
			mapper.map(0L, record, context);
		}
		reader.close();
		mapper.cleanup(context);
		collector.close();
	}

	class MapTaskContext extends LocalTaskContext implements TaskContext {

		private OutputCollector collector;

		public MapTaskContext(LocalJobConf conf, TaskId id,
				OutputCollector collector) {
			super(conf, id);
			this.collector = collector;
		}

		@Override
		public void write(Record key, Record value) throws IOException {
			collector.write(key, value);
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
