package com.aliyun.odps.mapred.local.conf;

import com.aliyun.odps.Column;
import com.aliyun.odps.conf.Configuration;
import com.aliyun.odps.data.TableInfo;
import com.aliyun.odps.mapred.conf.JobConf;
import com.aliyun.odps.mapred.utils.SchemaUtils;

public class LocalConf extends JobConf {

	public LocalConf(Configuration conf) {
		super(conf);
	}

}
