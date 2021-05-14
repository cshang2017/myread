

package org.apache.flink.table.runtime.generated;

import org.apache.flink.api.common.functions.Function;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.TableAggregateFunction;
import org.apache.flink.table.runtime.dataview.StateDataViewStore;

/**
 * The base class for handling aggregate or table aggregate functions.
 *
 * <p>It is code generated to handle all {@link AggregateFunction}s and
 * {@link TableAggregateFunction}s together in an aggregation.
 *
 * <p>It is the entry point for aggregate operators to operate all {@link AggregateFunction}s and
 * {@link TableAggregateFunction}s.
 */
public interface AggsHandleFunctionBase extends Function {

	/**
	 * Initialization method for the function. It is called before the actual working methods.
	 */
	void open(StateDataViewStore store) throws Exception;

	/**
	 * Accumulates the input values to the accumulators.
	 * @param input input values bundled in a row
	 */
	void accumulate(RowData input) throws Exception;

	/**
	 * Retracts the input values from the accumulators.
	 * @param input input values bundled in a row
	 */
	void retract(RowData input) throws Exception;

	/**
	 * Merges the other accumulators into current accumulators.
	 *
	 * @param accumulators The other row of accumulators
	 */
	void merge(RowData accumulators) throws Exception;

	/**
	 * Set the current accumulators (saved in a row) which contains the current aggregated results.
	 * In streaming: accumulators are store in the state, we need to restore aggregate buffers from state.
	 * In batch: accumulators are store in the hashMap, we need to restore aggregate buffers from hashMap.
	 *
	 * @param accumulators current accumulators
	 */
	void setAccumulators(RowData accumulators) throws Exception;

	/**
	 * Resets all the accumulators.
	 */
	void resetAccumulators() throws Exception;

	/**
	 * Gets the current accumulators (saved in a row) which contains the current
	 * aggregated results.
	 * @return the current accumulators
	 */
	RowData getAccumulators() throws Exception;

	/**
	 * Initializes the accumulators and save them to a accumulators row.
	 *
	 * @return a row of accumulators which contains the aggregated results
	 */
	RowData createAccumulators() throws Exception;

	/**
	 * Cleanup for the retired accumulators state.
	 */
	void cleanup() throws Exception;

	/**
	 * Tear-down method for this function. It can be used for clean up work.
	 * By default, this method does nothing.
	 */
	void close() throws Exception;
}
