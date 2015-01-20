package edu.thu.mapred.local.map;

import java.io.IOException;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.TaskId;

public interface OutputCollector {
	public void collect(Record key, Record value) throws IOException;

	public void flush() throws IOException;

	public void close();

	public void init(TaskId id) throws Exception;
}
