package edu.thu.mapred.local.io;

import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;

import com.aliyun.odps.Column;
import com.aliyun.odps.data.Record;

import edu.thu.mapred.local.LocalRecord;
import edu.thu.mapred.local.util.RecordComparator;

public class LocalRecordIterator implements Iterator<Record> {
	protected RawRecordIterator in;
	private LocalRecord key;
	private LocalRecord nextKey;
	private LocalRecord value;
	private boolean hasNext;
	private boolean more;
	private RecordComparator comparator;
	private DataInputBuffer keyIn = new DataInputBuffer();
	private DataInputBuffer valueIn = new DataInputBuffer();

	public LocalRecordIterator(RawRecordIterator in, RecordComparator comparator, Column[] keySchema,
			Column[] valueSchema) throws IOException {
		this.in = in;
		this.comparator = comparator;

		this.nextKey = new LocalRecord(keySchema);
		this.value = new LocalRecord(valueSchema);

		readNextKey();
		this.key = this.nextKey;
		this.nextKey = new LocalRecord(keySchema);
		this.hasNext = this.more;
	}

	@Override
	public boolean hasNext() {
		return this.hasNext;
	}

	@Override
	public Record next() {
		if (!this.hasNext) {
			throw new NoSuchElementException();
		}
		try {
			readNextValue();
			readNextKey();
		} catch (IOException ie) {
			throw new RuntimeException(ie);
		}
		return this.value;
	}

	@Override
	public void remove() {
		throw new UnsupportedOperationException("unimplemented");
	}

	public void nextKey() throws IOException {
		while (this.hasNext) {
			readNextKey();
		}

		LocalRecord tmpKey = this.key;
		this.key = this.nextKey;
		this.nextKey = tmpKey;
		this.hasNext = this.more;
	}

	public boolean more() {
		return this.more;
	}

	public Record getKey() {
		return this.key;
	}

	private void readNextKey() throws IOException {
		this.more = this.in.next();
		if (this.more) {
			DataInputBuffer nextKeyBytes = this.in.getKey();
			this.keyIn.reset(nextKeyBytes.getData(), nextKeyBytes.getPosition(), nextKeyBytes.getLength()
					- nextKeyBytes.getPosition());
			this.nextKey.deserialize(this.keyIn);
			this.hasNext = this.key != null && (this.comparator.compare(this.key, this.nextKey) == 0);
		} else {
			this.hasNext = false;
		}
	}

	private void readNextValue() throws IOException {
		DataInputBuffer nextValueBytes = this.in.getValue();
		this.valueIn.reset(nextValueBytes.getData(), nextValueBytes.getPosition(),
				nextValueBytes.getLength() - nextValueBytes.getPosition());
		this.value.deserialize(this.valueIn);
	}

}
