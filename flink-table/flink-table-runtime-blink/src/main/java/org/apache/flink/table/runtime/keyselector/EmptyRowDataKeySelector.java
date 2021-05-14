package org.apache.flink.table.runtime.keyselector;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryRowDataUtil;
import org.apache.flink.table.runtime.typeutils.RowDataTypeInfo;

/**
 * A utility class which key is always empty no matter what the input row is.
 */
public class EmptyRowDataKeySelector implements RowDataKeySelector {

	public static final EmptyRowDataKeySelector INSTANCE = new EmptyRowDataKeySelector();

	private final RowDataTypeInfo returnType = new RowDataTypeInfo();

	@Override
	public RowData getKey(RowData value) throws Exception {
		return BinaryRowDataUtil.EMPTY_ROW;
	}

	@Override
	public RowDataTypeInfo getProducedType() {
		return returnType;
	}
}
