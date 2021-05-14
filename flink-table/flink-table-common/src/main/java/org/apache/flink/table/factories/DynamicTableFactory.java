package org.apache.flink.table.factories;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;

/**
 * Base interface for configuring a dynamic table connector for an external storage system from catalog
 * and session information.
 *
 * <p>Dynamic tables are the core concept of Flink's Table & SQL API for processing both bounded and
 * unbounded data in a unified fashion.
 *
 * <p>Implement {@link DynamicTableSourceFactory} for constructing a {@link DynamicTableSource}.
 *
 * <p>Implement {@link DynamicTableSinkFactory} for constructing a {@link DynamicTableSink}.
 *
 * <p>The options {@link FactoryUtil#PROPERTY_VERSION} and {@link FactoryUtil#CONNECTOR} are implicitly
 * added and must not be declared.
 */
@PublicEvolving
public interface DynamicTableFactory extends Factory {

	/**
	 * Provides catalog and session information describing the dynamic table to be accessed.
	 */
	interface Context {

		/**
		 * Returns the identifier of the table in the {@link Catalog}.
		 */
		ObjectIdentifier getObjectIdentifier();

		/**
		 * Returns table information received from the {@link Catalog}.
		 */
		CatalogTable getCatalogTable();

		/**
		 * Gives read-only access to the configuration of the current session.
		 */
		ReadableConfig getConfiguration();

		/**
		 * Returns the class loader of the current session.
		 *
		 * <p>The class loader is in particular useful for discovering further (nested) factories.
		 */
		ClassLoader getClassLoader();
	}
}
