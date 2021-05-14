package org.apache.flink.table.sources;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.table.functions.AsyncTableFunction;
import org.apache.flink.table.functions.TableFunction;

/**
 * A {@link TableSource} which supports for lookup accessing via key column(s).
 * For example, MySQL TableSource can implement this interface to support lookup accessing.
 * When temporal join this MySQL table, the runtime behavior could be in a lookup fashion.
 *
 * @param <T> type of the result
 */
@Experimental
public interface LookupableTableSource<T> extends TableSource<T> {

	/**
	 * Gets the {@link TableFunction} which supports lookup one key at a time.
	 * @param lookupKeys the chosen field names as lookup keys, it is in the defined order
	 */
	TableFunction<T> getLookupFunction(String[] lookupKeys);

	/**
	 * Gets the {@link AsyncTableFunction} which supports async lookup one key at a time.
	 * @param lookupKeys the chosen field names as lookup keys, it is in the defined order
	 */
	AsyncTableFunction<T> getAsyncLookupFunction(String[] lookupKeys);

	/**
	 * Returns true if async lookup is enabled.
	 *
	 * <p>The lookup function returned by {@link #getAsyncLookupFunction(String[])} will be
	 * used if returns true. Otherwise, the lookup function returned by
	 * {@link #getLookupFunction(String[])} will be used.
	 */
	boolean isAsyncEnabled();
}
