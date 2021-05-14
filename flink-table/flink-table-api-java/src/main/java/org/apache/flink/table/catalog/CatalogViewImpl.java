package org.apache.flink.table.catalog;

import org.apache.flink.table.api.TableSchema;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * An implementation of catalog view.
 */
public class CatalogViewImpl extends AbstractCatalogView {
	public CatalogViewImpl(
			String originalQuery,
			String expandedQuery,
			TableSchema schema,
			Map<String, String> properties,
			String comment) {
		super(originalQuery, expandedQuery, schema, properties, comment);
	}

	@Override
	public CatalogBaseTable copy() {
		return new CatalogViewImpl(
			getOriginalQuery(),
			getExpandedQuery(),
			getSchema().copy(),
			new HashMap<>(getProperties()),
			getComment()
		);
	}

	@Override
	public Optional<String> getDescription() {
		return Optional.ofNullable(getComment());
	}

	@Override
	public Optional<String> getDetailedDescription() {
		return Optional.of("This is a catalog view implementation");
	}
}
