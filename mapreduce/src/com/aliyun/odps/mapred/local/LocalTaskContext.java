package com.aliyun.odps.mapred.local;

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
import com.aliyun.odps.mapred.local.conf.LocalJobConf;

public class LocalTaskContext implements TaskContext {

	private LocalJobConf conf;

	private TaskId taskId;

	public LocalTaskContext(LocalJobConf conf, TaskId id) {
		this.conf = conf;
		this.taskId = id;
	}

	@Override
	public JobConf getJobConf() {
		return conf;
	}

	@Override
	public int getNumReduceTasks() {
		return 0;
	}

	@Override
	public Column[] getMapOutputKeySchema() {
		return conf.getMapOutputKeySchema();
	}

	@Override
	public Column[] getMapOutputValueSchema() {
		return conf.getMapOutputValueSchema();
	}

	@Override
	public Class<? extends Mapper> getMapperClass() throws ClassNotFoundException {
		return conf.getMapperClass();
	}

	@Override
	public Class<? extends Reducer> getCombinerClass()
			throws ClassNotFoundException {
		return conf.getCombinerClass();
	}

	@Override
	public Class<? extends Reducer> getReducerClass()
			throws ClassNotFoundException {
		return conf.getReducerClass();
	}

	@Override
	public String[] getGroupingColumns() {
		return conf.getOutputGroupingColumns();
	}

	@Override
	public TaskId getTaskID() {
		return this.taskId;
	}

	@Override
	public TableInfo[] getOutputTableInfo() throws IOException {
		return null;
	}

	@Override
	public Record createOutputRecord() throws IOException {
		Record record = new LocalRecord(conf.getOutputSchema());

		return record;
	}

	@Override
	public Record createOutputRecord(String label) throws IOException {

		return null;

	}

	@Override
	public Record createOutputKeyRecord() throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Record createOutputValueRecord() throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Record createMapOutputKeyRecord() throws IOException {
		Record record = new LocalRecord(conf.getMapOutputKeySchema());
		return record;
	}

	@Override
	public Record createMapOutputValueRecord() throws IOException {
		Record record = new LocalRecord(conf.getMapOutputValueSchema());
		return record;
	}

	@Override
	public BufferedInputStream readResourceFileAsStream(String resourceName)
			throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Iterator<Record> readResourceTable(String resourceName)
			throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Counter getCounter(Enum<?> name) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Counter getCounter(String group, String name) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void progress() {
		// TODO Auto-generated method stub

	}

	@Override
	public void write(Record record) throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public void write(Record record, String label) throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public void write(Record key, Record value) throws IOException {
		// TODO Auto-generated method stub

	}

}
