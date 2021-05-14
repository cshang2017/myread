package org.apache.flink.table.runtime.generated;

import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Collector;

/**
 * The base class for handling table aggregate functions with namespace.
 */
public interface NamespaceTableAggsHandleFunction<N> extends NamespaceAggsHandleFunctionBase<N> {

	/**
	 * Emits the result of the aggregation from the current accumulators and namespace
	 * properties (like window start).
	 *
	 * @param namespace the namespace properties which should be calculated, such window start
	 * @param key       the group key for the current emit.
	 * @param out       the collector used to emit results.
	 */
	void emitValue(N namespace, RowData key, Collector<RowData> out) throws Exception;
}
