package edu.thu.mapred.local.io;

public interface Sortable {
	int compare(int i, int j);

	void swap(int i, int j);
}
