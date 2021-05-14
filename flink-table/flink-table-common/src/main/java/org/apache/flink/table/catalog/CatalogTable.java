package org.apache.flink.table.catalog;

import java.util.List;
import java.util.Map;

/**
 * Represents a table in a catalog.
 */
public interface CatalogTable extends CatalogBaseTable {
	/**
	 * Check if the table is partitioned or not.
	 *
	 * @return true if the table is partitioned; otherwise, false
	 */
	boolean isPartitioned();

	/**
	 * Get the partition keys of the table. This will be an empty set if the table is not partitioned.
	 *
	 * @return partition keys of the table
	 */
	List<String> getPartitionKeys();

	/**
	 * Returns a copy of this {@code CatalogTable} with given table options {@code options}.
	 *
	 * @return a new copy of this table with replaced table options
	 */
	CatalogTable copy(Map<String, String> options);

	/**
	 * Serializes this instance into a map of string-based properties.
	 *
	 * <p>Compared to the pure table options in {@link #getOptions()}, the map includes schema,
	 * partitioning, and other characteristics in a serialized form.
	 */
	Map<String, String> toProperties();
}
