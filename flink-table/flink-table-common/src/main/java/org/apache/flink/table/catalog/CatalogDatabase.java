

package org.apache.flink.table.catalog;

import java.util.Map;
import java.util.Optional;

/**
 * Interface of a database in a catalog.
 */
public interface CatalogDatabase {
	/**
	 * Get a map of properties associated with the database.
	 */
	Map<String, String> getProperties();

	/**
	 * Get comment of the database.
	 *
	 * @return comment of the database
	 */
	String getComment();

	/**
	 * Get a deep copy of the CatalogDatabase instance.
	 *
	 * @return a copy of CatalogDatabase instance
	 */
	CatalogDatabase copy();

	/**
	 * Get a brief description of the database.
	 *
	 * @return an optional short description of the database
	 */
	Optional<String> getDescription();

	/**
	 * Get a detailed description of the database.
	 *
	 * @return an optional long description of the database
	 */
	Optional<String> getDetailedDescription();
}
