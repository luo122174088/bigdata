package edu.thu.mapred.local.io;

import java.io.IOException;

import edu.thu.mapred.local.LocalJobConf;

public class MemorySegment extends RecordSegment {

	protected byte[] buffer;

	protected int buf_start;

	protected int buf_end;

	public MemorySegment(LocalJobConf conf, byte[] buffer, int start, int end) throws IOException {
		super(conf);
		this.buffer = buffer;
		this.buf_start = start;
		this.buf_end = end;
	}

	@Override
	public void init() throws IOException {
		if (this.reader == null) {
			reader = new MemoryRecordReader(buffer, buf_start, buf_end - buf_start);
		}
	}

	@Override
	public long getLength() {
		return buf_end - buf_start;
	}
}
