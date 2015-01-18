package edu.thu.mapred.local.util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class IOUtil {

	public static void writeVInt(DataOutput stream, int i) throws IOException {
		writeVLong(stream, i);
	}

	public static void writeVLong(DataOutput stream, long i) throws IOException {
		if (i >= -112 && i <= 127) {
			stream.writeByte((byte) i);
			return;
		}

		int len = -112;
		if (i < 0) {
			i ^= -1L;
			len = -120;
		}

		long tmp = i;
		while (tmp != 0) {
			tmp = tmp >> 8;
			len--;
		}

		stream.writeByte((byte) len);

		len = (len < -120) ? -(len + 120) : -(len + 112);

		for (int idx = len; idx != 0; idx--) {
			int shiftbits = (idx - 1) * 8;
			long mask = 0xFFL << shiftbits;
			stream.writeByte((byte) ((i & mask) >> shiftbits));
		}
	}

	public static long readVLong(DataInput stream) throws IOException {
		byte firstByte = stream.readByte();
		int len = decodeVIntSize(firstByte);
		if (len == 1) {
			return firstByte;
		}
		long i = 0;
		for (int idx = 0; idx < len - 1; idx++) {
			byte b = stream.readByte();
			i = i << 8;
			i = i | (b & 0xFF);
		}
		return (isNegativeVInt(firstByte) ? (i ^ -1L) : i);
	}

	public static int readVInt(DataInput in) throws IOException {
		return (int) readVLong(in);
	}

	public static long readVLong(byte[] in, int start) {
		byte firstByte = in[start++];
		int len = decodeVIntSize(firstByte);
		if (len == 1) {
			return firstByte;
		}
		long i = 0;
		for (int idx = 0; idx < len - 1; idx++) {
			byte b = in[start++];
			i = i << 8;
			i = i | (b & 0xFF);
		}
		return (isNegativeVInt(firstByte) ? (i ^ -1L) : i);
	}

	public static int readVInt(byte[] in, int start) {
		return (int) readVLong(in, start);
	}

	public static boolean isNegativeVInt(byte value) {
		return value < -120 || (value >= -112 && value < 0);
	}

	public static int decodeVIntSize(byte value) {
		if (value >= -112) {
			return 1;
		} else if (value < -120) {
			return -119 - value;
		}
		return -111 - value;
	}

	public static int getVLongSize(long i) {
		if (i >= -112 && i <= 127) {
			return 1;
		}

		if (i < 0) {
			i ^= -1L;
		}
		int dataBits = Long.SIZE - Long.numberOfLeadingZeros(i);
		return (dataBits + 7) / 8 + 1;
	}

	public static long readLong(byte[] bytes, int start) {
		return ((long) (readInt(bytes, start)) << 32) + (readInt(bytes, start + 4) & 0xFFFFFFFFL);
	}

	public static int readInt(byte[] bytes, int start) {
		return (((bytes[start] & 0xff) << 24) + ((bytes[start + 1] & 0xff) << 16)
				+ ((bytes[start + 2] & 0xff) << 8) + ((bytes[start + 3] & 0xff)));

	}

	public static int compareBytes(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
		int end1 = s1 + l1;
		int end2 = s2 + l2;
		for (int i = s1, j = s2; i < end1 && j < end2; i++, j++) {
			int a = (b1[i] & 0xff);
			int b = (b2[j] & 0xff);
			if (a != b) {
				return a - b;
			}
		}
		return l1 - l2;
	}
}
