package edu.thu.mapred.local.io;

import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;

public class DataOutputBuffer extends DataOutputStream {

	private static class Buffer extends ByteArrayOutputStream {
		public byte[] getData() {
			return this.buf;
		}

		public int getLength() {
			return this.count;
		}

		public Buffer() {
			super();
		}

		public Buffer(int size) {
			super(size);
		}

		public void write(DataInput in, int len) throws IOException {
			int newcount = this.count + len;
			if (newcount > this.buf.length) {
				byte newbuf[] = new byte[Math.max(this.buf.length << 1, newcount)];
				System.arraycopy(this.buf, 0, newbuf, 0, this.count);
				this.buf = newbuf;
			}
			in.readFully(this.buf, this.count, len);
			this.count = newcount;
		}
	}

	private Buffer buffer;

	/** Constructs a new empty buffer. */
	public DataOutputBuffer() {
		this(new Buffer());
	}

	public DataOutputBuffer(int size) {
		this(new Buffer(size));
	}

	private DataOutputBuffer(Buffer buffer) {
		super(buffer);
		this.buffer = buffer;
	}

	public byte[] getData() {
		return this.buffer.getData();
	}

	public int getLength() {
		return this.buffer.getLength();
	}

	public DataOutputBuffer reset() {
		this.written = 0;
		this.buffer.reset();
		return this;
	}

	public void write(DataInput in, int length) throws IOException {
		this.buffer.write(in, length);
	}

	public void writeTo(OutputStream out) throws IOException {
		this.buffer.writeTo(out);
	}
}
