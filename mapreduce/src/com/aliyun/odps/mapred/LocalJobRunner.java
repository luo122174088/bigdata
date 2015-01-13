package com.aliyun.odps.mapred;

import com.aliyun.odps.OdpsException;
import com.aliyun.odps.conf.Configuration;
import com.aliyun.odps.mapred.conf.JobConf;
import com.aliyun.odps.mapred.local.LocalRunningJob;
import com.aliyun.odps.mapred.local.conf.LocalJobConf;

public class LocalJobRunner implements JobRunner {

	private LocalJobConf conf;

	public LocalJobRunner(JobConf conf) {
		this.conf = new LocalJobConf(conf);
	}

	public LocalJobRunner() {
	}

	@Override
	public Configuration getConf() {
		return conf;
	}

	@Override
	public void setConf(Configuration conf) {
		this.conf = new LocalJobConf(conf);
	}

	@Override
	public RunningJob submit() throws OdpsException {
		RunningJob job = new LocalRunningJob(conf);
		return job;
	}

}
