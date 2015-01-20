package edu.thu.mapred.local.io;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;

public class DataInputBuffer extends DataInputStream {
	public static class Buffer extends ByteArrayInputStream {
		public Buffer() {
			super(new byte[] {});
		}

		public void reset(byte[] input, int start, int length) {
			this.buf = input;
			this.count = start + length;
			this.mark = start;
			this.pos = start;
		}

		public byte[] getData() {
			return this.buf;
		}

		public int getPosition() {
			return this.pos;
		}

		public int getLength() {
			return this.count;
		}
	}

	private Buffer buffer;

	public DataInputBuffer() {
		this(new Buffer());
	}

	public DataInputBuffer(Buffer buffer) {
		super(buffer);
		this.buffer = buffer;
	}

	public void reset(byte[] input, int length) {
		this.buffer.reset(input, 0, length);
	}

	public void reset(byte[] input, int start, int length) {
		this.buffer.reset(input, start, length);
	}

	public byte[] getData() {
		return this.buffer.getData();
	}

	public int getPosition() {
		return this.buffer.getPosition();
	}

	public int getLength() {
		return this.buffer.getLength();
	}

}
