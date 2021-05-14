package org.apache.flink.table.sources;

import org.apache.flink.annotation.Experimental;

/**
 * Adds support for limiting push-down to a {@link TableSource}.
 * A {@link TableSource} extending this interface is able to limit the number of records.
 *
 * <p>After pushing down, source only needs to try its best to limit the number of output records,
 * but does not need to guarantee that the number must be less than or equal to the limit.
 */
@Experimental
public interface LimitableTableSource<T> {

	/**
	 * Return the flag to indicate whether limit push down has been tried. Must return true on
	 * the returned instance of {@link #applyLimit(long)}.
	 */
	boolean isLimitPushedDown();

	/**
	 * Check and push down the limit to the table source.
	 *
	 * @param limit the value which limit the number of records.
	 * @return A new cloned instance of {@link TableSource}.
	 */
	TableSource<T> applyLimit(long limit);
}
