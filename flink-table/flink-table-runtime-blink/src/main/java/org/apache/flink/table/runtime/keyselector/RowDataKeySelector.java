package org.apache.flink.table.runtime.keyselector;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.RowDataTypeInfo;

/**
 * RowDataKeySelector takes an RowData and extracts the deterministic key for the RowData.
 */
public interface RowDataKeySelector extends KeySelector<RowData, RowData>, ResultTypeQueryable<RowData> {

	RowDataTypeInfo getProducedType();

}
