package org.apache.flink.table.data;

import org.apache.flink.table.data.binary.TypedSetters;
import org.apache.flink.table.data.vector.BytesColumnVector.Bytes;
import org.apache.flink.table.data.vector.VectorizedColumnBatch;
import org.apache.flink.types.RowKind;

/**
 * Columnar row to support access to vector column data. It is a row view in {@link VectorizedColumnBatch}.
 */
public final class ColumnarRowData implements RowData, TypedSetters {

	private RowKind rowKind = RowKind.INSERT;
	private VectorizedColumnBatch vectorizedColumnBatch;
	private int rowId;

	public ColumnarRowData() {}

	public ColumnarRowData(VectorizedColumnBatch vectorizedColumnBatch) {
		this(vectorizedColumnBatch, 0);
	}

	public ColumnarRowData(VectorizedColumnBatch vectorizedColumnBatch, int rowId) {
		this.vectorizedColumnBatch = vectorizedColumnBatch;
		this.rowId = rowId;
	}

	public void setVectorizedColumnBatch(
			VectorizedColumnBatch vectorizedColumnBatch) {
		this.vectorizedColumnBatch = vectorizedColumnBatch;
		this.rowId = 0;
	}

	public void setRowId(int rowId) {
		this.rowId = rowId;
	}

	@Override
	public RowKind getRowKind() {
		return rowKind;
	}

	@Override
	public void setRowKind(RowKind kind) {
		this.rowKind = kind;
	}

	@Override
	public int getArity() {
		return vectorizedColumnBatch.getArity();
	}

	@Override
	public boolean isNullAt(int pos) {
		return vectorizedColumnBatch.isNullAt(rowId, pos);
	}

	@Override
	public boolean getBoolean(int pos) {
		return vectorizedColumnBatch.getBoolean(rowId, pos);
	}

	@Override
	public byte getByte(int pos) {
		return vectorizedColumnBatch.getByte(rowId, pos);
	}

	@Override
	public short getShort(int pos) {
		return vectorizedColumnBatch.getShort(rowId, pos);
	}

	@Override
	public int getInt(int pos) {
		return vectorizedColumnBatch.getInt(rowId, pos);
	}

	@Override
	public long getLong(int pos) {
		return vectorizedColumnBatch.getLong(rowId, pos);
	}

	@Override
	public float getFloat(int pos) {
		return vectorizedColumnBatch.getFloat(rowId, pos);
	}

	@Override
	public double getDouble(int pos) {
		return vectorizedColumnBatch.getDouble(rowId, pos);
	}

	@Override
	public StringData getString(int pos) {
		Bytes byteArray = vectorizedColumnBatch.getByteArray(rowId, pos);
		return StringData.fromBytes(byteArray.data, byteArray.offset, byteArray.len);
	}

	@Override
	public DecimalData getDecimal(int pos, int precision, int scale) {
		return vectorizedColumnBatch.getDecimal(rowId, pos, precision, scale);
	}

	@Override
	public TimestampData getTimestamp(int pos, int precision) {
		return vectorizedColumnBatch.getTimestamp(rowId, pos, precision);
	}

	@Override
	public <T> RawValueData<T> getRawValue(int pos) {
		throw new UnsupportedOperationException("RawValueData is not supported.");
	}

	@Override
	public byte[] getBinary(int pos) {
		Bytes byteArray = vectorizedColumnBatch.getByteArray(rowId, pos);
		if (byteArray.len == byteArray.data.length) {
			return byteArray.data;
		} else {
			byte[] ret = new byte[byteArray.len];
			System.arraycopy(byteArray.data, byteArray.offset, ret, 0, byteArray.len);
			return ret;
		}
	}

	@Override
	public RowData getRow(int pos, int numFields) {
		return vectorizedColumnBatch.getRow(rowId, pos);
	}

	@Override
	public ArrayData getArray(int pos) {
		return vectorizedColumnBatch.getArray(rowId, pos);
	}

	@Override
	public MapData getMap(int pos) {
		throw new UnsupportedOperationException("Map is not supported.");
	}

	@Override
	public void setNullAt(int pos) {
		throw new UnsupportedOperationException("Not support the operation!");
	}

	@Override
	public void setBoolean(int pos, boolean value) {
		throw new UnsupportedOperationException("Not support the operation!");
	}

	@Override
	public void setByte(int pos, byte value) {
		throw new UnsupportedOperationException("Not support the operation!");
	}

	@Override
	public void setShort(int pos, short value) {
		throw new UnsupportedOperationException("Not support the operation!");
	}

	@Override
	public void setInt(int pos, int value) {
		throw new UnsupportedOperationException("Not support the operation!");
	}

	@Override
	public void setLong(int pos, long value) {
		throw new UnsupportedOperationException("Not support the operation!");
	}

	@Override
	public void setFloat(int pos, float value) {
		throw new UnsupportedOperationException("Not support the operation!");
	}

	@Override
	public void setDouble(int pos, double value) {
		throw new UnsupportedOperationException("Not support the operation!");
	}

	@Override
	public void setDecimal(int pos, DecimalData value, int precision) {
		throw new UnsupportedOperationException("Not support the operation!");
	}

	@Override
	public void setTimestamp(int pos, TimestampData value, int precision) {
		throw new UnsupportedOperationException("Not support the operation!");
	}

	@Override
	public boolean equals(Object o) {
		throw new UnsupportedOperationException(
				"ColumnarRowData do not support equals, please compare fields one by one!");
	}

	@Override
	public int hashCode() {
		throw new UnsupportedOperationException(
				"ColumnarRowData do not support hashCode, please hash fields one by one!");
	}
}
