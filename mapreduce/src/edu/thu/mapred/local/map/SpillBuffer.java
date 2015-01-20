package edu.thu.mapred.local.map;

import java.io.IOException;
import java.io.OutputStream;

import edu.thu.mapred.local.LocalJobConf;

class SpillBuffer extends OutputStream {
	byte[] buffer;
	int buf_start = 0;
	int buf_end = 0;
	boolean full = false;

	public SpillBuffer(LocalJobConf conf) {
		int size = conf.getSpillBufferMB();
		size = size * 1024 * 1024;
		buffer = new byte[size];
	}

	@Override
	public void write(int b) throws IOException {
		buffer[buf_end++] = (byte) b;
	}

	public void mark() {
		this.buf_start = buf_end;
	}

	@Override
	public void write(byte[] b, int off, int len) throws IOException {
		System.arraycopy(b, off, buffer, buf_end, len);
		buf_end += len;
	}

}
