package org.apache.flink.table.factories;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ObjectIdentifier;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Implementation of {@link TableSinkFactory.Context}.
 */
@Internal
public class TableSinkFactoryContextImpl implements TableSinkFactory.Context {

	private final ObjectIdentifier identifier;
	private final CatalogTable table;
	private final ReadableConfig config;
	private final boolean isBounded;

	public TableSinkFactoryContextImpl(
			ObjectIdentifier identifier,
			CatalogTable table,
			ReadableConfig config,
			boolean isBounded) {
		this.identifier = checkNotNull(identifier);
		this.table = checkNotNull(table);
		this.config = checkNotNull(config);
		this.isBounded = isBounded;
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

	@Override
	public boolean isBounded() {
		return isBounded;
	}
}
