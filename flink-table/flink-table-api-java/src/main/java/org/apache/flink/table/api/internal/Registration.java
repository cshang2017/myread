package org.apache.flink.table.api.internal;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.catalog.CatalogBaseTable;

/**
 * A way to register a table in a {@link TableEnvironment} that this descriptor originates from.
 */
@Internal
public interface Registration {

	/**
	 * Creates a temporary table in a given path.
	 *
	 * @param path Path where to register the given table
	 * @param table table to register
	 */
	void createTemporaryTable(String path, CatalogBaseTable table);
}
