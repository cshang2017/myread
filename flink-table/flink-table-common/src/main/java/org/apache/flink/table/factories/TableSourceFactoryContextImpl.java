
package org.apache.flink.table.factories;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ObjectIdentifier;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Implementation of {@link TableSourceFactory.Context}.
 */
@Internal
public class TableSourceFactoryContextImpl implements TableSourceFactory.Context {

	private final ObjectIdentifier identifier;
	private final CatalogTable table;
	private final ReadableConfig config;

	public TableSourceFactoryContextImpl(
			ObjectIdentifier identifier,
			CatalogTable table,
			ReadableConfig config) {
		this.identifier = checkNotNull(identifier);
		this.table = checkNotNull(table);
		this.config = checkNotNull(config);
	}

	@Override
	public ObjectIdentifier getObjectIdentifier() {
		return identifier;
	}

	@Override
	public CatalogTable getTable() {
		return table;
	}

	@Override
	public ReadableConfig getConfiguration() {
		return config;
	}
}
