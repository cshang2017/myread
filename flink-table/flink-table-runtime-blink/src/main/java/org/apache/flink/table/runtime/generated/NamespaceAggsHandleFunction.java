
package org.apache.flink.table.runtime.generated;

import org.apache.flink.table.data.RowData;

/**
 * The base class for handling aggregate functions with namespace.
 */
public interface NamespaceAggsHandleFunction<N> extends NamespaceAggsHandleFunctionBase<N> {

	/**
	 * Gets the result of the aggregation from the current accumulators and
	 * namespace properties (like window start).
	 *
	 * @param namespace the namespace properties which should be calculated, such window start
	 * @return the final result (saved in a row) of the current accumulators.
	 */
	RowData getValue(N namespace) throws Exception;
}
