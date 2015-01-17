package edu.thu.mapred.local;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.util.Iterator;

import com.aliyun.odps.Column;
import com.aliyun.odps.counter.Counter;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.TableInfo;
import com.aliyun.odps.mapred.Mapper;
import com.aliyun.odps.mapred.Reducer;
import com.aliyun.odps.mapred.TaskContext;
import com.aliyun.odps.mapred.TaskId;
import com.aliyun.odps.mapred.conf.JobConf;

public abstract class LocalTaskContext implements TaskContext {

	private LocalJobConf conf;

	private TaskId taskId;

	public LocalTaskContext(LocalJobConf conf, TaskId id) {
		this.conf = conf;
		this.taskId = id;
	}

	@Override
	public JobConf getJobConf() {
		return this.conf;
	}

	@Override
	public int getNumReduceTasks() {
		throw new UnsupportedOperationException("Unimplemented");
	}

	@Override
	public Column[] getMapOutputKeySchema() {
		return this.conf.getMapOutputKeySchema();
	}

	@Override
	public Column[] getMapOutputValueSchema() {
		return this.conf.getMapOutputValueSchema();
	}

	@Override
	public Class<? extends Mapper> getMapperClass() throws ClassNotFoundException {
		return this.conf.getMapperClass();
	}

	@Override
	public Class<? extends Reducer> getCombinerClass() throws ClassNotFoundException {
		return this.conf.getCombinerClass();
	}

	@Override
	public Class<? extends Reducer> getReducerClass() throws ClassNotFoundException {
		return this.conf.getReducerClass();
	}

	@Override
	public String[] getGroupingColumns() {
		throw new UnsupportedOperationException("Unimplemented");
	}

	@Override
	public TaskId getTaskID() {
		return this.taskId;
	}

	@Override
	public TableInfo[] getOutputTableInfo() throws IOException {
		throw new UnsupportedOperationException("Unimplemented");
	}

	@Override
	public Record createOutputRecord() throws IOException {
		Record record = new LocalRecord(this.conf.getOutputSchema());

		return record;
	}

	@Override
	public Record createOutputRecord(String label) throws IOException {
		throw new UnsupportedOperationException("Unimplemented");
	}

	@Override
	public Record createOutputKeyRecord() throws IOException {
		throw new UnsupportedOperationException("Unimplemented");
	}

	@Override
	public Record createOutputValueRecord() throws IOException {
		throw new UnsupportedOperationException("Unimplemented");
	}

	@Override
	public Record createMapOutputKeyRecord() throws IOException {
		return new LocalRecord(this.conf.getMapOutputKeySchema());

	}

	@Override
	public Record createMapOutputValueRecord() throws IOException {
		return new LocalRecord(this.conf.getMapOutputValueSchema());
	}

	@Override
	public BufferedInputStream readResourceFileAsStream(String resourceName) throws IOException {
		throw new UnsupportedOperationException("Unimplemented");
	}

	@Override
	public Iterator<Record> readResourceTable(String resourceName) throws IOException {
		throw new UnsupportedOperationException("Unimplemented");
	}

	@Override
	public Counter getCounter(Enum<?> name) {
		throw new UnsupportedOperationException("Unimplemented");
	}

	@Override
	public Counter getCounter(String group, String name) {
		throw new UnsupportedOperationException("Unimplemented");
	}

	@Override
	public void progress() {
		throw new UnsupportedOperationException("Unimplemented");
	}

	@Override
	public void write(Record record) throws IOException {
		throw new UnsupportedOperationException("Unimplemented");
	}

	@Override
	public void write(Record record, String label) throws IOException {
		throw new UnsupportedOperationException("Unimplemented");

	}

	@Override
	public void write(Record key, Record value) throws IOException {
		throw new UnsupportedOperationException("Unimplemented");

	}

}
