package edu.thu.mapred.local;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Date;
import java.util.HashMap;

import com.aliyun.odps.Column;
import com.aliyun.odps.OdpsType;
import com.aliyun.odps.data.Record;

import edu.thu.mapred.local.util.IOUtil;

public class LocalRecord implements Record {

	private Column[] columns;
	private Object[] values;

	private HashMap<String, Integer> names = new HashMap<String, Integer>();

	public LocalRecord(int len) {
		this.columns = new Column[len];
		this.values = new Object[len];
	}

	public LocalRecord(Column[] columns) {
		if (columns == null) {
			throw new IllegalArgumentException();
		}
		this.columns = columns;
		values = new Object[columns.length];
		for (int i = 0; i < columns.length; i++) {
			names.put(columns[i].getName(), i);
		}

	}

	@Override
	public int getColumnCount() {
		return values.length;
	}

	@Override
	public Column[] getColumns() {
		return columns;
	}

	@Override
	public void set(int idx, Object value) {
		values[idx] = value;
	}

	@Override
	public Object get(int idx) {
		return values[idx];
	}

	@Override
	public void set(String columnName, Object value) {
		set(getColumnIndex(columnName), value);
	}

	@Override
	public Object get(String columnName) {
		return values[getColumnIndex(columnName)];
	}

	@Override
	public void setBigint(int idx, Long value) {
		values[idx] = value;
	}

	@Override
	public Long getBigint(int idx) {
		return (Long) get(idx);
	}

	@Override
	public void setBigint(String columnName, Long value) {
		setBigint(getColumnIndex(columnName), value);
	}

	@Override
	public Long getBigint(String columnName) {
		return (Long) get(columnName);
	}

	@Override
	public void setDouble(int idx, Double value) {
		values[idx] = value;
	}

	@Override
	public Double getDouble(int idx) {
		return (Double) get(idx);
	}

	@Override
	public void setDouble(String columnName, Double value) {
		setDouble(getColumnIndex(columnName), value);
	}

	@Override
	public Double getDouble(String columnName) {
		return (Double) get(columnName);
	}

	@Override
	public void setBoolean(int idx, Boolean value) {
		values[idx] = value;
	}

	@Override
	public Boolean getBoolean(int idx) {
		return (Boolean) get(idx);
	}

	@Override
	public void setBoolean(String columnName, Boolean value) {
		setBoolean(getColumnIndex(columnName), value);
	}

	@Override
	public Boolean getBoolean(String columnName) {
		return (Boolean) get(columnName);
	}

	@Override
	public void setDatetime(int idx, Date value) {
		values[idx] = value;
	}

	@Override
	public Date getDatetime(int idx) {
		return (Date) get(idx);
	}

	@Override
	public void setDatetime(String columnName, Date value) {
		setDatetime(getColumnIndex(columnName), value);
	}

	@Override
	public Date getDatetime(String columnName) {
		return (Date) get(columnName);
	}

	@Override
	public void setString(int idx, String value) {
		values[idx] = value;
	}

	@Override
	public String getString(int idx) {
		return (String) values[idx];
	}

	@Override
	public void setString(String columnName, String value) {
		setString(getColumnIndex(columnName), value);
	}

	@Override
	public String getString(String columnName) {
		return getString(getColumnIndex(columnName));
	}

	@Override
	public void setString(int idx, byte[] value) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void setString(String columnName, byte[] value) {
		setString(getColumnIndex(columnName), value);
	}

	@Override
	public byte[] getBytes(int idx) {
		return (byte[]) get(idx);
	}

	@Override
	public byte[] getBytes(String columnName) {
		return getBytes(columnName);
	}

	@Override
	public void set(Object[] values) {
		if (values == null || columns.length != values.length) {
			throw new IllegalArgumentException();
		}
		for (int i = 0; i < values.length; ++i) {
			this.values[i] = values[i];
		}
	}

	@Override
	public Object[] toArray() {
		return values;
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		for (Object value : values) {
			sb.append(value);
		}
		return sb.toString();
	}

	private int getColumnIndex(String name) {
		Integer idx = names.get(name);
		if (idx == null) {
			throw new IllegalArgumentException("No such column:" + name);
		}
		return idx;
	}

	public void serialize(DataOutput out) throws IOException {
		for (int i = 0; i < values.length; i++) {
			OdpsType type = columns[i].getType();
			switch (type) {
			case BIGINT:
				out.writeLong((Long) values[i]);
				break;
			case STRING:
				String value = (String) values[i];
				byte[] bs = value.getBytes();
				out.writeInt(bs.length);
				out.write(bs);
				break;
			default:
				throw new UnsupportedOperationException("Unimplemented");
			}

		}
	}

	public void deserialize(DataInput in) throws IOException {
		for (int i = 0; i < values.length; i++) {
			OdpsType type = columns[i].getType();
			switch (type) {
			case BIGINT:
				values[i] = in.readLong();
				break;
			case STRING:
				int len = in.readInt();
				byte[] bs = new byte[len];
				in.readFully(bs);
				values[i] = new String(bs);
				break;
			default:
				throw new UnsupportedOperationException("Unimplemented");
			}
		}
	}

	public void fastSet(Object[] values) {
		this.values = values;
	}

	public static class DefaultRecordComparator implements RawRecordComparator {
		private Column[] schema;

		public DefaultRecordComparator(Column[] schema) {
			this.schema = schema;
		}

		@Override
		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
			for (int i = 0; i < schema.length; i++) {
				switch (schema[i].getType()) {
				case BIGINT:
					long v1 = IOUtil.readLong(b1, s1);
					long v2 = IOUtil.readLong(b2, s2);
					if (v1 < v2) {
						return -1;
					} else if (v1 > v2) {
						return 1;
					} else {
						s1 += 8;
						s2 += 8;

						l1 -= 8;
						l2 -= 8;
					}
					break;
				case STRING:
					int n1 = IOUtil.readInt(b1, s1);
					int n2 = IOUtil.readInt(b2, s2);
					int result = IOUtil.compareBytes(b1, s1 + 4, n1, b2, s2 + 4, n2);
					if (result != 0) {
						return result;
					} else {
						s1 = s1 + 4 + n1;
						s2 = s2 + 4 + n1;
						l1 = l1 - 4 - n1;
						l2 = l2 - 4 - n2;
					}
					break;
				default:
					throw new UnsupportedOperationException("Unimplemented");
				}
			}
			return 0;
		}

		@Override
		public int compare(LocalRecord o1, LocalRecord o2) {
			for (int i = 0; i < schema.length; i++) {
				switch (schema[i].getType()) {
				case BIGINT:
					long v1 = o1.getBigint(i);
					long v2 = o2.getBigint(i);
					if (v1 != v2) {
						return v1 < v2 ? -1 : 1;
					}
					break;
				case STRING:
					String s1 = o1.getString(i);
					String s2 = o2.getString(i);
					int r = s1.compareTo(s2);
					if (r != 0) {
						return r;
					}
					break;
				default:
					throw new UnsupportedOperationException("Unimplemented");
				}
			}
			return 0;
		}

	}

}