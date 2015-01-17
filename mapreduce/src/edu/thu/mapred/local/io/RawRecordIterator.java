package edu.thu.mapred.local.io;

import java.io.IOException;

public interface RawRecordIterator {

	DataInputBuffer getKey() throws IOException;

	DataInputBuffer getValue() throws IOException;

	boolean next() throws IOException;

	void close() throws IOException;

}
