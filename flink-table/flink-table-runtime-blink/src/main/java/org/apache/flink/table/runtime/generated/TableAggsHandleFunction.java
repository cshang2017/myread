package org.apache.flink.table.runtime.generated;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.TableAggregateFunction;
import org.apache.flink.util.Collector;

/**
 * The base class for handling table aggregate functions.
 *
 * <p>It is code generated to handle all {@link TableAggregateFunction}s together in an aggregation.
 *
 * <p>It is the entry point for aggregate operators to operate all {@link TableAggregateFunction}s.
 */
public interface TableAggsHandleFunction extends AggsHandleFunctionBase {

	/**
	 * Emit the result of the table aggregation through the collector.
	 *
	 * @param out        the collector used to emit records.
	 * @param currentKey the current group key.
	 * @param isRetract  the retraction flag which indicates whether emit retract values.
	 */
	void emitValue(Collector<RowData> out, RowData currentKey, boolean isRetract) throws Exception;
}
