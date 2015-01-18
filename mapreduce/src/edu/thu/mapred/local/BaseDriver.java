package edu.thu.mapred.local;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.aliyun.odps.mapred.TaskId;

import edu.thu.mapred.local.io.TaskFileHelper;

public abstract class BaseDriver implements Runnable {

	protected LocalJobConf conf;
	protected TaskId id;
	protected List<File> mapFiles;
	protected TaskFileHelper fileHelper;

	protected static Logger logger = LoggerFactory.getLogger(BaseDriver.class);

	public BaseDriver(LocalJobConf conf) {
		this(conf, null);
	}

	public BaseDriver(LocalJobConf conf, List<File> mapFiles) {
		this.conf = conf;
		this.mapFiles = mapFiles;
		this.fileHelper = new TaskFileHelper(this);
	}

	/**
	 * Reuse
	 * 
	 * @param id
	 * @throws Exception
	 */
	protected void init(TaskId id) throws IOException {
		this.id = id;
		if (id != null) {
			File dir = new File(getTaskDir());
			if (!dir.exists()) {
				dir.mkdir();
			}
		}

	}

	public abstract String getTaskDir();

	public abstract void runInternal() throws Exception;

	@Override
	public final void run() {
		try {
			runInternal();
		} catch (Exception e) {
			logger.error("Task {} fails.", this.id, e);
			throw new RuntimeException(e);
		}
	}

}
