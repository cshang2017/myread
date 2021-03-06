package org.apache.flink.table.connector.source;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.types.RowKind;

import java.io.Serializable;

/**
 * A {@link DynamicTableSource} that looks up rows of an external storage system by one or more keys
 * during runtime.
 *
 * <p>Compared to {@link ScanTableSource}, the source does not have to read the entire table and can
 * lazily fetch individual values from a (possibly continuously changing) external table when necessary.
 *
 * <p>Note: Compared to {@link ScanTableSource}, a {@link LookupTableSource} does only support emitting
 * insert-only changes currently (see also {@link RowKind}). Further abilities are not supported.
 *
 * <p>In the last step, the planner will call {@link #getLookupRuntimeProvider(LookupContext)} for obtaining a
 * provider of runtime implementation. The key fields that are required to perform a lookup are derived
 * from a query by the planner and will be provided in the given {@link LookupContext#getKeys()}. The values
 * for those key fields are passed during runtime.
 */
@Experimental
public interface LookupTableSource extends DynamicTableSource {

	/**
	 * Returns a provider of runtime implementation for reading the data.
	 *
	 * <p>There exist different interfaces for runtime implementation which is why {@link LookupRuntimeProvider}
	 * serves as the base interface.
	 *
	 * <p>Independent of the provider interface, a source implementation can work on either arbitrary
	 * objects or internal data structures (see {@link org.apache.flink.table.data} for more information).
	 *
	 * <p>The given {@link LookupContext} offers utilities by the planner for creating runtime implementation
	 * with minimal dependencies to internal data structures.
	 *
	 * @see TableFunctionProvider
	 * @see AsyncTableFunctionProvider
	 */
	LookupRuntimeProvider getLookupRuntimeProvider(LookupContext context);

	// --------------------------------------------------------------------------------------------
	// Helper interfaces
	// --------------------------------------------------------------------------------------------

	/**
	 * Context for creating runtime implementation via a {@link LookupRuntimeProvider}.
	 *
	 * <p>It offers utilities by the planner for creating runtime implementation with minimal dependencies
	 * to internal data structures.
	 *
	 * <p>Methods should be called in {@link #getLookupRuntimeProvider(LookupContext)}. Returned instances
	 * that are {@link Serializable} can be directly passed into the runtime implementation class.
	 */
	interface LookupContext extends DynamicTableSource.Context {

		/**
		 * Returns an array of key index paths that should be used during the lookup. The indices are
		 * 0-based and support composite keys within (possibly nested) structures.
		 *
		 * <p>For example, given a table with data type {@code ROW < i INT, s STRING, r ROW < i2 INT, s2 STRING > >},
		 * this method would return {@code [[0], [2, 1]]} when {@code i} and {@code s2} are used for performing
		 * a lookup.
		 *
		 * @return array of key index paths
		 */
		int[][] getKeys();
	}

	/**
	 * Provides actual runtime implementation for reading the data.
	 *
	 * <p>There exist different interfaces for runtime implementation which is why {@link LookupRuntimeProvider}
	 * serves as the base interface.
	 *
	 * @see TableFunctionProvider
	 * @see AsyncTableFunctionProvider
	 */
	interface LookupRuntimeProvider {
		// marker interface
	}
}
