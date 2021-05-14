package org.apache.flink.table.data.util;

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.types.RowKind;

/**
 * Utilities for {@link RowData}.
 */
public final class RowDataUtil {

	/**
	 * Returns true if the message is either {@link RowKind#INSERT} or {@link RowKind#UPDATE_AFTER},
	 * which refers to an accumulate operation of aggregation.
	 */
	public static boolean isAccumulateMsg(RowData row) {
		RowKind kind = row.getRowKind();
		return kind == RowKind.INSERT || kind == RowKind.UPDATE_AFTER;
	}

	/**
	 * Returns true if the message is either {@link RowKind#DELETE} or {@link RowKind#UPDATE_BEFORE},
	 * which refers to a retract operation of aggregation.
	 */
	public static boolean isRetractMsg(RowData row) {
		RowKind kind = row.getRowKind();
		return kind == RowKind.UPDATE_BEFORE || kind == RowKind.DELETE;
	}

	public static GenericRowData toGenericRow(
			RowData row,
			LogicalType[] types) {
		if (row instanceof GenericRowData) {
			return (GenericRowData) row;
		} else {
			GenericRowData newRow = new GenericRowData(row.getArity());
			newRow.setRowKind(row.getRowKind());
			for (int i = 0; i < row.getArity(); i++) {
				if (row.isNullAt(i)) {
					newRow.setField(i, null);
				} else {
					newRow.setField(i, RowData.get(row, i, types[i]));
				}
			}
			return newRow;
		}
	}
}
