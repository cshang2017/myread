package org.apache.flink.table.runtime.generated;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.AggregateFunction;

/**
 * The base class for handling aggregate functions.
 *
 * <p>It is code generated to handle all {@link AggregateFunction}s together in an aggregation.
 *
 * <p>It is the entry point for aggregate operators to operate all {@link AggregateFunction}s.
 */
public interface AggsHandleFunction extends AggsHandleFunctionBase {

	/**
	 * Gets the result of the aggregation from the current accumulators.
	 * @return the final result (saved in a row) of the current accumulators.
	 */
	RowData getValue() throws Exception;
}
