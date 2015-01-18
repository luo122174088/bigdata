package edu.thu.mapred.local;

import com.aliyun.odps.Column;
import com.aliyun.odps.conf.Configuration;
import com.aliyun.odps.mapred.conf.JobConf;
import com.aliyun.odps.mapred.utils.SchemaUtils;

import edu.thu.mapred.local.util.RecordComparator;

public class LocalJobConf extends JobConf {

	private static final String LOCAL_JOB_DIR = "local.job.dir";

	private static final String OUTPUT_SCHEMA = "output.schema";

	private static final String INPUT_PATH = "input.path";

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
		return get(INPUT_PATH);
	}

	public void setMapInputPath(String path) {
		set(INPUT_PATH, path);
	}

	public Column[] getOutputSchema() {
		String schema = get(OUTPUT_SCHEMA);
		if (schema == null) {
			schema = "word:string,count:bigint";
			set(OUTPUT_SCHEMA, schema);
		}
		return SchemaUtils.fromString(schema);
	}

	public int getMapThreads() {
		return 2;
	}

	public int getSortFactor() {
		return 10;
	}

	public RecordComparator getMapOutputKeyComparator() {
		return new LocalRecord.DefaultRecordComparator(getMapOutputKeySchema());
	}

	public float getSpiller() {
		return getFloat("io.spiller", 0.8f);
	}

	public int getSortMB() {
		return getInt("io.sort.mb", 100);
	}

}
