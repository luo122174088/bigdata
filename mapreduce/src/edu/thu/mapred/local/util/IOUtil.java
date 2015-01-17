package edu.thu.mapred.local.util;

public class IOUtil {

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
