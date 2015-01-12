package com.aliyun.odps.mapred.local;

import java.io.IOException;

import com.aliyun.odps.counter.Counters;
import com.aliyun.odps.mapred.JobStatus;
import com.aliyun.odps.mapred.RunningJob;
import com.aliyun.odps.mapred.local.conf.LocalConf;

public class LocalRunningJob implements RunningJob {

	private LocalConf conf;

	public LocalRunningJob(LocalConf conf) {
		this.conf = conf;
	}

	@Override
	public void waitForCompletion() {
		//TODO
	}

	@Override
	public String getInstanceID() {
		return null;
	}

	@Override
	public boolean isComplete() {
		return false;
	}

	@Override
	public boolean isSuccessful() {
		return false;
	}

	@Override
	public JobStatus getJobStatus() {
		return null;
	}

	@Override
	public void killJob() {

	}

	@Override
	public Counters getCounters() {
		return null;
	}

	@Override
	public String getDiagnostics() {
		return null;
	}

	@Override
	public float mapProgress() throws IOException {
		return 0;
	}

	@Override
	public float reduceProgress() throws IOException {
		return 0;
	}

}
