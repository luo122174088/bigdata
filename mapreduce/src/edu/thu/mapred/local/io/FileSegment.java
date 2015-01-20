package edu.thu.mapred.local.io;

import java.io.File;
import java.io.IOException;

import edu.thu.mapred.local.LocalJobConf;

public class FileSegment extends RecordSegment {

	protected boolean preserve;
	protected File file;

	public FileSegment(LocalJobConf conf, File file, boolean preserve) throws IOException {
		super(conf);
		this.file = file;
		this.preserve = preserve;
	}

	@Override
	public void init() throws IOException {
		if (this.reader == null) {
			this.reader = new LocalRecordReader(this.conf, this.file);
		}
	}

	@Override
	public void close() throws IOException {
		super.close();
		if (!this.preserve) {
			boolean result = this.file.delete();
			if (!result) {
				logger.warn("Fail to delete " + file.getAbsolutePath());
			}
		}
	}

	@Override
	public long getLength() {
		return file.length();
	}

	@Override
	public String toString() {
		return file.getName();
	}

}
