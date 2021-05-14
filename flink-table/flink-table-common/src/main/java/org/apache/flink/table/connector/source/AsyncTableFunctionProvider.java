package org.apache.flink.table.connector.source;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.functions.AsyncTableFunction;

/**
 * Provider of an {@link AsyncTableFunction} instance as a runtime implementation for {@link LookupTableSource}.
 *
 * <p>The runtime will call the function with values describing the table's lookup keys (in the order
 * of declaration in {@link LookupTableSource.Context#getKeys()}).
 */
@PublicEvolving
public interface AsyncTableFunctionProvider<T> extends LookupTableSource.LookupRuntimeProvider {

	/**
	 * Helper method for creating a static provider.
	 */
	static <T> AsyncTableFunctionProvider<T> of(AsyncTableFunction<T> asyncTableFunction) {
		return () -> asyncTableFunction;
	}

	/**
	 * Creates a {@link AsyncTableFunction} instance.
	 */
	AsyncTableFunction<T> createAsyncTableFunction();
}
