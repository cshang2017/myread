package org.apache.flink.table.factories;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.connector.sink.DynamicTableSink;

/**
 * Creates a {@link DynamicTableSink} instance from a {@link CatalogTable} and additional context
 * information.
 *
 * <p>See {@link Factory} for more information about the general design of a factory.
 */
@PublicEvolving
public interface DynamicTableSinkFactory extends DynamicTableFactory {

	/**
	 * Creates a {@link DynamicTableSink} instance from a {@link CatalogTable} and additional context
	 * information.
	 *
	 * <p>An implementation should perform validation and the discovery of further (nested) factories
	 * in this method.
	 */
	DynamicTableSink createDynamicTableSink(Context context);
}
