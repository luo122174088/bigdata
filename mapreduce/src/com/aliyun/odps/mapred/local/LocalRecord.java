package com.aliyun.odps.mapred.local;

import java.util.Date;
import java.util.HashMap;

import com.aliyun.odps.Column;
import com.aliyun.odps.data.Record;

public class LocalRecord implements Record {

	private Column[] columns;
	private final Object[] values;

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

}
