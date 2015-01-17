package edu.thu.mapred.local.util;

import java.util.Comparator;

import edu.thu.mapred.local.LocalRecord;

public interface RecordComparator extends Comparator<LocalRecord> {

	public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2);

}
