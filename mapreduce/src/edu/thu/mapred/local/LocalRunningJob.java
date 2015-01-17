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

import com.aliyun.odps.counter.Counters;
import com.aliyun.odps.mapred.JobStatus;
import com.aliyun.odps.mapred.RunningJob;
import com.aliyun.odps.mapred.TaskId;

import edu.thu.mapred.local.map.InputSplit;
import edu.thu.mapred.local.map.MapDriver;
import edu.thu.mapred.local.red.ReduceDriver;

public class LocalRunningJob implements RunningJob {

	private static Logger logger = LoggerFactory.getLogger(LocalRunningJob.class);

	private LocalJobConf conf;

	private String jobId;

	private List<InputSplit> inputFiles = new LinkedList<InputSplit>();
	private List<TaskId> mapIds = new LinkedList<TaskId>();

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

		String inputPath = this.conf.getMapInputPath();
		long start = System.currentTimeMillis();
		long end = 0;
		List<File> mapFiles = Collections.synchronizedList(new ArrayList<File>());
		File dir = new File(inputPath);
		try {
			logger.info("Map phase starts.");
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
				InputSplit split = new InputSplit(f);
				TaskId mapId = new TaskId("M1", id++);
				this.inputFiles.add(split);
				this.mapIds.add(mapId);
			}
			int threads = Math.min(this.inputFiles.size(), this.conf.getMapThreads());
			ExecutorService mapService = Executors.newFixedThreadPool(threads);
			for (int i = 0; i < this.conf.getMapThreads(); i++) {
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
			Long mapEnd = System.currentTimeMillis();
			logger.info("Map phase ends in {}ms.", (mapEnd - start));
			logger.info("Reduce phase starts.");
			id = 1;
			TaskId redId = new TaskId("R1", id++);
			ReduceDriver reduceDriver = new ReduceDriver(this.conf, redId, mapFiles);
			reduceDriver.run();
			Long reduceEnd = System.currentTimeMillis();
			logger.info("Reduce phase ends in {}ms", (reduceEnd - mapEnd));
		} catch (Exception e) {
			logger.error("Job faisl.", e);
		} finally {
			cleanUp();
			end = System.currentTimeMillis();
			logger.info("Job ends in {}ms", (end - start));
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
