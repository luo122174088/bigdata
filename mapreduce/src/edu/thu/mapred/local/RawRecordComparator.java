package edu.thu.mapred.local;

import java.util.Comparator;

public interface RawRecordComparator extends Comparator<LocalRecord> {

	public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2);

}
