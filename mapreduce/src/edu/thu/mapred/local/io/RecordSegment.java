package edu.thu.mapred.local.io;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.thu.mapred.local.LocalJobConf;

public abstract class RecordSegment {
	protected static Logger logger = LoggerFactory.getLogger(RecordSegment.class);

	protected LocalRecordReader reader = null;
	protected DataInputBuffer key = new DataInputBuffer();
	protected DataInputBuffer value = new DataInputBuffer();
	protected LocalJobConf conf;

	public RecordSegment(LocalJobConf conf) throws IOException {
		this.conf = conf;
	}

	public abstract void init() throws IOException;

	public abstract long getLength();

	public DataInputBuffer getKey() {
		return this.key;
	}

	public DataInputBuffer getValue() {
		return this.value;
	}

	public boolean next() throws IOException {
		return this.reader.next(this.key, this.value);
	}

	public void close() throws IOException {
		if (this.reader != null) {
			this.reader.close();
			this.reader = null;
		}

	}
}