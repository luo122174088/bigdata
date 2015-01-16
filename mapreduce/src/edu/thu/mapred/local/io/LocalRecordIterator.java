package edu.thu.mapred.local.io;

import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;

import com.aliyun.odps.Column;
import com.aliyun.odps.data.Record;

import edu.thu.mapred.local.LocalRecord;
import edu.thu.mapred.local.RawRecordComparator;
import edu.thu.mapred.local.RawRecordIterator;

public class LocalRecordIterator implements Iterator<Record> {
	protected RawRecordIterator in;
	private LocalRecord key;
	private LocalRecord nextKey;
	private LocalRecord value;
	private boolean hasNext;
	private boolean more;
	private RawRecordComparator comparator;
	private DataInputBuffer keyIn = new DataInputBuffer();
	private DataInputBuffer valueIn = new DataInputBuffer();

	public LocalRecordIterator(RawRecordIterator in, RawRecordComparator comparator,
			Column[] keySchema, Column[] valueSchema) throws IOException {
		this.in = in;
		this.comparator = comparator;

		nextKey = new LocalRecord(keySchema);
		value = new LocalRecord(valueSchema);

		readNextKey();
		key = nextKey;
		nextKey = new LocalRecord(keySchema);
		hasNext = more;
	}

	RawRecordIterator getRawIterator() {
		return in;
	}

	public boolean hasNext() {
		return hasNext;
	}

	public Record next() {
		if (!hasNext) {
			throw new NoSuchElementException();
		}
		try {
			readNextValue();
			readNextKey();
		} catch (IOException ie) {
			throw new RuntimeException(ie);
		}
		return value;
	}

	public void remove() {
		throw new RuntimeException("not implemented");
	}

	public void nextKey() throws IOException {
		while (hasNext) {
			readNextKey();
		}

		LocalRecord tmpKey = key;
		key = nextKey;
		nextKey = tmpKey;
		hasNext = more;
	}

	public boolean more() {
		return more;
	}

	public Record getKey() {
		return key;
	}

	private void readNextKey() throws IOException {
		more = in.next();
		if (more) {
			DataInputBuffer nextKeyBytes = in.getKey();
			keyIn.reset(nextKeyBytes.getData(), nextKeyBytes.getPosition(), nextKeyBytes.getLength()
					- nextKeyBytes.getPosition());
			nextKey.deserialize(keyIn);
			hasNext = key != null && (comparator.compare(key, nextKey) == 0);
		} else {
			hasNext = false;
		}
	}

	private void readNextValue() throws IOException {
		DataInputBuffer nextValueBytes = in.getValue();
		valueIn.reset(nextValueBytes.getData(), nextValueBytes.getPosition(),
				nextValueBytes.getLength() - nextValueBytes.getPosition());
		value.deserialize(valueIn);
	}

}
