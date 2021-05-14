package org.apache.flink.table.catalog;

import org.apache.flink.table.api.TableSchema;
import org.apache.flink.util.StringUtils;

import java.util.Map;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * An abstract catalog view.
 */
public abstract class AbstractCatalogView implements CatalogView {
	// Original text of the view definition.
	private final String originalQuery;

	// Expanded text of the original view definition
	// This is needed because the context such as current DB is
	// lost after the session, in which view is defined, is gone.
	// Expanded query text takes care of the this, as an example.
	private final String expandedQuery;

	private final TableSchema schema;
	private final Map<String, String> properties;
	private final String comment;

	public AbstractCatalogView(String originalQuery, String expandedQuery, TableSchema schema,
			Map<String, String> properties, String comment) {
		checkArgument(!StringUtils.isNullOrWhitespaceOnly(originalQuery), "originalQuery cannot be null or empty");
		checkArgument(!StringUtils.isNullOrWhitespaceOnly(expandedQuery), "expandedQuery cannot be null or empty");

		this.originalQuery = originalQuery;
		this.expandedQuery = expandedQuery;
		this.schema = checkNotNull(schema, "schema cannot be null");
		this.properties = checkNotNull(properties, "properties cannot be null");
		this.comment = comment;
	}

	@Override
	public String getOriginalQuery() {
		return this.originalQuery;
	}

	@Override
	public String getExpandedQuery() {
		return this.expandedQuery;
	}

	@Override
	public Map<String, String> getProperties() {
		return this.properties;
	}

	@Override
	public TableSchema getSchema() {
		return this.schema;
	}

	public String getComment() {
		return this.comment;
	}

}
