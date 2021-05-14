

package org.apache.flink.table.connector.source;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.functions.TableFunction;

/**
 * Provider of a {@link TableFunction} instance as a runtime implementation for {@link LookupTableSource}.
 *
 * <p>The runtime will call the function with values describing the table's lookup keys (in the order
 * of declaration in {@link LookupTableSource.Context#getKeys()}).
 */
@PublicEvolving
public interface TableFunctionProvider<T> extends LookupTableSource.LookupRuntimeProvider {

	/**
	 * Helper method for creating a static provider.
	 */
	static <T> TableFunctionProvider<T> of(TableFunction<T> tableFunction) {
		return () -> tableFunction;
	}

	/**
	 * Creates a {@link TableFunction} instance.
	 */
	TableFunction<T> createTableFunction();
}
