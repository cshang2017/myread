package org.apache.flink.table.runtime.generated;

import org.apache.flink.api.common.functions.Function;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.dataview.StateDataViewStore;

/**
 * The base class for handling aggregate or table aggregate functions.
 *
 * <p>The differences between {@link NamespaceAggsHandleFunctionBase} and
 * {@link AggsHandleFunctionBase} is that the {@link NamespaceAggsHandleFunctionBase} has namespace.
 *
 * @param <N> type of namespace
 */
public interface NamespaceAggsHandleFunctionBase<N> extends Function {

	/**
	 * Initialization method for the function. It is called before the actual working methods.
	 */
	void open(StateDataViewStore store) throws Exception;

	/**
	 * Set the current accumulators (saved in a row) which contains the current
	 * aggregated results.
	 * @param accumulators current accumulators
	 */
	void setAccumulators(N namespace, RowData accumulators) throws Exception;

	/**
	 * Accumulates the input values to the accumulators.
	 * @param inputRow input values bundled in a row
	 */
	void accumulate(RowData inputRow) throws Exception;

	/**
	 * Retracts the input values from the accumulators.
	 * @param inputRow input values bundled in a row
	 */
	void retract(RowData inputRow) throws Exception;

	/**
	 * Merges the other accumulators into current accumulators.
	 *
	 * @param otherAcc The other row of accumulators
	 */
	void merge(N namespace, RowData otherAcc) throws Exception;

	/**
	 * Initializes the accumulators and save them to a accumulators row.
	 *
	 * @return a row of accumulators which contains the aggregated results
	 */
	RowData createAccumulators() throws Exception;

	/**
	 * Gets the current accumulators (saved in a row) which contains the current
	 * aggregated results.
	 * @return the current accumulators
	 */
	RowData getAccumulators() throws Exception;

	/**
	 * Cleanup for the retired accumulators state.
	 */
	void cleanup(N namespace) throws Exception;

	/**
	 * Tear-down method for this function. It can be used for clean up work.
	 * By default, this method does nothing.
	 */
	void close() throws Exception;

}
