

package org.apache.flink.table.catalog;

import org.apache.flink.util.StringUtils;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * Abstract class for catalogs.
 */
public abstract class AbstractCatalog implements Catalog {
	private final String catalogName;
	private final String defaultDatabase;

	public AbstractCatalog(String name, String defaultDatabase) {
		checkArgument(!StringUtils.isNullOrWhitespaceOnly(name), "name cannot be null or empty");
		checkArgument(!StringUtils.isNullOrWhitespaceOnly(defaultDatabase), "defaultDatabase cannot be null or empty");

		this.catalogName = name;
		this.defaultDatabase = defaultDatabase;
	}

	public String getName() {
		return catalogName;
	}

	@Override
	public String getDefaultDatabase() {
		return defaultDatabase;
	}
}
