package org.apache.flink.table.catalog;

import org.apache.flink.table.api.TableSchema;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * An abstract catalog table.
 */
public abstract class AbstractCatalogTable implements CatalogTable {
	// Schema of the table (column names and types)
	private final TableSchema tableSchema;
	// Partition keys if this is a partitioned table. It's an empty set if the table is not partitioned
	private final List<String> partitionKeys;
	// Properties of the table
	private final Map<String, String> properties;
	// Comment of the table
	private final String comment;

	public AbstractCatalogTable(
			TableSchema tableSchema,
			Map<String, String> properties,
			String comment) {
		this(tableSchema, new ArrayList<>(), properties, comment);
	}

	public AbstractCatalogTable(
			TableSchema tableSchema,
			List<String> partitionKeys,
			Map<String, String> properties,
			String comment) {
		this.tableSchema = checkNotNull(tableSchema, "tableSchema cannot be null");
		this.partitionKeys = checkNotNull(partitionKeys, "partitionKeys cannot be null");
		this.properties = checkNotNull(properties, "properties cannot be null");

		checkArgument(
			properties.entrySet().stream().allMatch(e -> e.getKey() != null && e.getValue() != null),
			"properties cannot have null keys or values");

		this.comment = comment;
	}

	@Override
	public boolean isPartitioned() {
		return !partitionKeys.isEmpty();
	}

	@Override
	public List<String> getPartitionKeys() {
		return partitionKeys;
	}

	@Override
	public Map<String, String> getProperties() {
		return properties;
	}

	@Override
	public TableSchema getSchema() {
		return tableSchema;
	}

	@Override
	public String getComment() {
		return comment;
	}
}
