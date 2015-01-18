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
import edu.thu.mapred.local.io.CsvRecordReader;
import edu.thu.mapred.local.io.TaskFileHelper;

public class MapDriver extends BaseDriver {

	protected File inputFile;
	protected MapOutputCollector collector;

	protected List<File> inputFiles;
	protected List<TaskId> mapIds;

	public MapDriver(LocalJobConf conf, List<File> mapFiles, List<File> inputFiles,
			List<TaskId> mapIds) throws Exception {
		super(conf, mapFiles);
		this.collector = new MapOutputCollector(conf, new TaskFileHelper(this), mapFiles);
		this.inputFiles = inputFiles;
		this.mapIds = mapIds;
	}

	public void init(TaskId id, File file) throws Exception {
		super.init(id);
		this.collector.init(id);
		this.inputFile = file;
	};

	@Override
	public void runInternal() throws Exception {

		while (true) {
			// consume a map file
			synchronized (this.inputFiles) {
				if (this.inputFiles.size() == 0) {
					return;
				}
				init(this.mapIds.remove(0), this.inputFiles.remove(0));
			}

			logger.info("Map task {} starts.", this.id);
			long start = System.currentTimeMillis();
			Class<? extends Mapper> mapperClass = this.conf.getMapperClass();
			Mapper mapper = mapperClass.newInstance();

			CsvRecordReader reader = new CsvRecordReader(this.inputFile);
			MapTaskContext context = new MapTaskContext(this.conf, this.id, this.collector);
			mapper.setup(context);

			Record record = null;
			try {
				while ((record = reader.read()) != null) {
					mapper.map(0, record, context);
				}
			} finally {
				reader.close();
				this.collector.flush();
				this.collector.close();
				mapper.cleanup(context);
				long end = System.currentTimeMillis();
				logger.info("Map task {} ends in {}ms", this.id, (end - start));
			}
		}

	}

	@Override
	public String getTaskDir() {
		return this.conf.getMapDir() + this.id.toString() + "/";
	}

	class MapTaskContext extends LocalTaskContext implements TaskContext {

		private MapOutputCollector collector;

		public MapTaskContext(LocalJobConf conf, TaskId id, MapOutputCollector collector) {
			super(conf, id);
			this.collector = collector;
		}

		@Override
		public void write(Record key, Record value) throws IOException {
			this.collector.collect(key, value);
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
