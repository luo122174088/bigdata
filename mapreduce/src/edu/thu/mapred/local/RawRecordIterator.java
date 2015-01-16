package edu.thu.mapred.local;

import java.io.IOException;

import edu.thu.mapred.local.io.DataInputBuffer;

public interface RawRecordIterator {

	DataInputBuffer getKey() throws IOException;

	DataInputBuffer getValue() throws IOException;

	boolean next() throws IOException;

	void close() throws IOException;

}
