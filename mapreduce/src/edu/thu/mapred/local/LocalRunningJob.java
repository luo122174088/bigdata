package edu.thu.mapred.local;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sun.awt.windows.ThemeReader;

import com.aliyun.odps.counter.Counters;
import com.aliyun.odps.mapred.JobStatus;
import com.aliyun.odps.mapred.RunningJob;
import com.aliyun.odps.mapred.TaskId;

import edu.thu.mapred.local.map.MapDriver;
import edu.thu.mapred.local.red.ReduceDriver;

public class LocalRunningJob implements RunningJob {

	private static Logger logger = LoggerFactory.getLogger(LocalRunningJob.class);

	private LocalJobConf conf;

	private String jobId;

	private List<File> inputFiles = new LinkedList<File>();
	private List<TaskId> mapIds = new LinkedList<TaskId>();

	private List<File> mapFiles;

	private long jobStart;
	private long jobEnd;

	private long mapEnd;

	public LocalRunningJob(LocalJobConf conf) {
		this.conf = conf;
		this.jobId = "Job" + System.currentTimeMillis();

		String dirPath = "job/" + this.jobId + "/";
		File dir = new File(dirPath);
		if (!dir.exists()) {
			dir.mkdirs();
		}
		conf.setJobDir(dirPath);
		File mapDir = new File(conf.getMapDir());
		mapDir.mkdir();
		File reduceDir = new File(conf.getReduceDir());
		reduceDir.mkdir();

		logger.info("Job starts: {}", this.jobId);
	}

	@Override
	public void waitForCompletion() {
		jobStart = System.currentTimeMillis();
		try {
			map();
			reduce();

		} catch (Exception e) {
			logger.error("Job fails.", e);
		} finally {
			cleanUp();
			jobEnd = System.currentTimeMillis();
			logger.info("Job ends in {}ms", (jobEnd - jobStart));
		}
	}

	private void map() throws Exception {
		logger.info("Map phase starts.");
		String inputPath = this.conf.getMapInputPath();
		if (inputPath == null) {
			logger.error("No input found. Please specify job input with input.path property.");
			return;
		}
		mapFiles = Collections.synchronizedList(new ArrayList<File>());
		File dir = new File(inputPath);
		File[] files = dir.listFiles();
		if (files == null || files.length == 0) {
			logger.error("No input found.");
			return;
		}
		int id = 1;
		for (File f : files) {
			if (!f.getName().endsWith(".csv")) {
				continue;
			}
			TaskId mapId = new TaskId("M1", id++);
			this.inputFiles.add(f);
			this.mapIds.add(mapId);
		}
		if (this.inputFiles.size() == 0) {
			logger.error("No input found.");
			return;
		}
		logger.info("Map phase processing {} input files.", this.inputFiles.size());
		int threads = Math.min(this.inputFiles.size(), this.conf.getMapThreads());
		if (threads > 1) {
			logger.info("Map phase runs in {} threads.", threads);
			ExecutorService mapService = Executors.newFixedThreadPool(threads);
			for (int i = 0; i < threads; i++) {
				MapDriver driver = new MapDriver(this.conf, mapFiles, this.inputFiles, this.mapIds);
				mapService.submit(driver);
			}
			try {
				mapService.shutdown();
				mapService.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
			} catch (InterruptedException ie) {
				mapService.shutdownNow();
				throw ie;
			}
		} else {
			logger.info("Map phase fall back to single thread.");
			MapDriver driver = new MapDriver(this.conf, mapFiles, this.inputFiles, this.mapIds);
			driver.run();
		}
		mapEnd = System.currentTimeMillis();
		logger.info("Map phase ends in {}ms.", (mapEnd - jobStart));
	}

	private void reduce() throws Exception {
		logger.info("Reduce phase starts.");
		TaskId redId = new TaskId("R1", 1);
		ReduceDriver reduceDriver = new ReduceDriver(this.conf, redId, mapFiles);
		reduceDriver.run();
		Long reduceEnd = System.currentTimeMillis();
		logger.info("Reduce phase ends in {}ms", (reduceEnd - mapEnd));
	}

	private void cleanUp() {

	}

	@Override
	public String getInstanceID() {
		throw new UnsupportedOperationException("Unimplemented");
	}

	@Override
	public boolean isComplete() {
		throw new UnsupportedOperationException("Unimplemented");
	}

	@Override
	public boolean isSuccessful() {
		throw new UnsupportedOperationException("Unimplemented");
	}

	@Override
	public JobStatus getJobStatus() {
		throw new UnsupportedOperationException("Unimplemented");
	}

	@Override
	public void killJob() {
		throw new UnsupportedOperationException("Unimplemented");

	}

	@Override
	public Counters getCounters() {
		throw new UnsupportedOperationException("Unimplemented");
	}

	@Override
	public String getDiagnostics() {
		throw new UnsupportedOperationException("Unimplemented");
	}

	@Override
	public float mapProgress() throws IOException {
		throw new UnsupportedOperationException("Unimplemented");
	}

	@Override
	public float reduceProgress() throws IOException {
		throw new UnsupportedOperationException("Unimplemented");
	}

}
