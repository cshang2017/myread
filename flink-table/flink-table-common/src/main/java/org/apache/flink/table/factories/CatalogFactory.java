package org.apache.flink.table.factories;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.catalog.Catalog;

import java.util.Map;

/**
 * A factory to create configured catalog instances based on string-based properties. See
 * also {@link TableFactory} for more information.
 */
@PublicEvolving
public interface CatalogFactory extends TableFactory {

	/**
	 * Creates and configures a {@link Catalog} using the given properties.
	 *
	 * @param properties normalized properties describing an external catalog.
	 * @return the configured  catalog.
	 */
	Catalog createCatalog(String name, Map<String, String> properties);
}
