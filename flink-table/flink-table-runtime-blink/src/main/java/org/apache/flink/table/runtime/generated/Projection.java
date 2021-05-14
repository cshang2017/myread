package org.apache.flink.table.runtime.generated;

import org.apache.flink.table.data.RowData;

/**
 * Interface for code generated projection, which will map a RowData to another one.
 */
public interface Projection<IN extends RowData, OUT extends RowData> {

	OUT apply(IN row);

}
