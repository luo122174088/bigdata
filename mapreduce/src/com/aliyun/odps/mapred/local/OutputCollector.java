package com.aliyun.odps.mapred.local;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.TaskId;
import com.aliyun.odps.mapred.local.conf.LocalJobConf;

public class OutputCollector {

	private LocalJobConf conf;

	private String[] keys;
	private long[] values;
	private TaskId id;
	private int count = 0;
	private List<File> mapOutputFiles;

	public OutputCollector(LocalJobConf conf, TaskId id, List<File> mapOutputFiles) {
		this.conf = conf;
		this.id = id;

		this.keys = new String[1024];
		this.values = new long[1024];
		this.mapOutputFiles = mapOutputFiles;
	}

	public void write(Record key, Record value) {
		keys[count] = key.getString(0);
		values[count++] = value.getBigint(0);
	}

	public void flush() {
		// do nothing
	}

	public void close() throws IOException {
		File file = new File(conf.getMapDir(), id.toString());

		sort(keys, values, 0, count - 1);
		DataOutputStream output = new DataOutputStream(new BufferedOutputStream(
				new FileOutputStream(file)));
		for (int i = 0; i < count; i++) {
			output.writeUTF(keys[i]);
			output.writeLong(values[i]);
		}
		output.close();

		mapOutputFiles.add(file);

	}

	private void sort(String[] key, long[] value, int left, int right) {
		if (left < right) {
			int q = partition(key, value, left, right);
			sort(key, value, left, q - 1);
			sort(key, value, q + 1, right);
		}

	}

	private int partition(String[] key, long[] value, int left, int right) {
		int i = left - 1;
		for (int j = left; j < right; j++) {
			if (key[j].compareTo(key[right]) <= 0) {
				i++;
				swap(key, value, i, j);
			}
		}
		swap(key, value, i + 1, right);
		return i + 1;
	}

	private void swap(String[] key, long[] value, int i, int j) {
		String t = key[i];
		key[i] = key[j];
		key[j] = t;

		long l = value[i];
		value[i] = value[j];
		value[j] = l;
	}

}
