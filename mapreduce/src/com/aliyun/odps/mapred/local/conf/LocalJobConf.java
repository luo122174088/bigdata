package com.aliyun.odps.mapred.local.conf;

import com.aliyun.odps.Column;
import com.aliyun.odps.conf.Configuration;
import com.aliyun.odps.mapred.conf.JobConf;
import com.aliyun.odps.mapred.utils.SchemaUtils;

public class LocalJobConf extends JobConf {

	static final String LOCAL_JOB_DIR = "local.job.dir";

	static final String OUTPUT_SCHEMA = "output.schema";

	public LocalJobConf(Configuration conf) {
		super(conf);
	}

	public String getJobDir() {
		return get(LOCAL_JOB_DIR);
	}

	public String getMapDir() {
		return get(LOCAL_JOB_DIR) + "map/";
	}

	public String getReduceDir() {
		return get(LOCAL_JOB_DIR) + "reduce/";
	}

	public void setJobDir(String dir) {
		set(LOCAL_JOB_DIR, dir);
	}

	public String getMapInputPath() {
		return "data/";
	}

	public Column[] getOutputSchema() {
		String schema = get(OUTPUT_SCHEMA);
		if (schema == null) {
			schema = "word:string,count:bigint";
			set(OUTPUT_SCHEMA, schema);
		}
		return SchemaUtils.fromString(schema);
	}
}
