package edu.thu.mapred.local;

import java.io.File;
import java.util.List;

import com.aliyun.odps.mapred.TaskId;

public abstract class BaseDriver {

	protected LocalJobConf conf;
	protected TaskId id;
	protected List<File> mapFiles;
	protected TaskFileHelper fileHelper;

	public BaseDriver(LocalJobConf conf, TaskId id) {
		this(conf, id, null);
		this.fileHelper = new TaskFileHelper(this);
	}

	public BaseDriver(LocalJobConf conf, TaskId id, List<File> mapFiles) {
		this.conf = conf;
		this.id = id;
		this.mapFiles = mapFiles;

		File dir = new File(getTaskDir());
		dir.mkdir();
	}

	public String getTaskDir() {
		return conf.getMapDir() + id.toString() + "/";
	}

	public abstract void run() throws Exception;

}
