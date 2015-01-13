package com.aliyun.odps.mapred.local;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.aliyun.odps.counter.Counters;
import com.aliyun.odps.mapred.JobStatus;
import com.aliyun.odps.mapred.RunningJob;
import com.aliyun.odps.mapred.TaskId;
import com.aliyun.odps.mapred.local.conf.LocalJobConf;

public class LocalRunningJob implements RunningJob {

	private LocalJobConf conf;

	private String jobId;

	public LocalRunningJob(LocalJobConf conf) {
		this.conf = conf;
		this.jobId = "Job" + System.currentTimeMillis();

		String dirPath = "job/" + jobId + "/";
		File dir = new File(dirPath);
		if (!dir.exists()) {
			dir.mkdirs();
		}
		conf.setJobDir(dirPath);

		File mapDir = new File(conf.getMapDir());
		mapDir.mkdir();
		File reduceDir = new File(conf.getReduceDir());
		reduceDir.mkdir();
	}

	@Override
	public void waitForCompletion() {

		String inputPath = conf.getMapInputPath();

		List<File> mapFiles = Collections.synchronizedList(new ArrayList<File>());
		File dir = new File(inputPath);
		try {

			File[] files = dir.listFiles();
			int id = 1;
			for (File file : files) {
				if (!file.getName().endsWith(".csv")) {
					continue;
				}
				InputSplit split = new InputSplit(file);
				TaskId mapId = new TaskId("M1", id++);
				MapDriver mapDriver = new MapDriver(conf, mapId, split, mapFiles);
				mapDriver.run();
			}
			id = 1;
			TaskId redId = new TaskId("R1", id++);
			ReduceDriver reduceDriver = new ReduceDriver(conf, redId, mapFiles);
			reduceDriver.run();
			cleanUp();
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	private void cleanUp() {

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
