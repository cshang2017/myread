

package org.apache.flink.table.catalog;

import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.factories.DynamicTableFactory;

import java.util.Map;
import java.util.Optional;

/**
 * CatalogBaseTable is the common parent of table and view. It has a map of
 * key-value pairs defining the properties of the table.
 */
public interface CatalogBaseTable {

	/**
	 * @deprecated Use {@link #getOptions()}.
	 */
	@Deprecated
	Map<String, String> getProperties();

	/**
	 * Returns a map of string-based options.
	 *
	 * <p>In case of {@link CatalogTable}, these options may determine the kind of connector and its
	 * configuration for accessing the data in the external system. See {@link DynamicTableFactory}
	 * for more information.
	 */
	default Map<String, String> getOptions() {
		return getProperties();
	}

	/**
	 * Get the schema of the table.
	 *
	 * @return schema of the table/view.
	 */
	TableSchema getSchema();

	/**
	 * Get comment of the table or view.
	 *
	 * @return comment of the table/view.
	 */
	String getComment();

	/**
	 * Get a deep copy of the CatalogBaseTable instance.
	 *
	 * @return a copy of the CatalogBaseTable instance
	 */
	CatalogBaseTable copy();

	/**
	 * Get a brief description of the table or view.
	 *
	 * @return an optional short description of the table/view
	 */
	Optional<String> getDescription();

	/**
	 * Get a detailed description of the table or view.
	 *
	 * @return an optional long description of the table/view
	 */
	Optional<String> getDetailedDescription();
}
