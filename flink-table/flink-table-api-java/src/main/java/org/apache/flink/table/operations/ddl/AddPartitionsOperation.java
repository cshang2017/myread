
package org.apache.flink.table.operations.ddl;

import org.apache.flink.table.catalog.CatalogPartition;
import org.apache.flink.table.catalog.CatalogPartitionSpec;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.operations.OperationUtils;

import java.util.List;
import java.util.Map;

/**
 * Operation to describe ALTER TABLE ADD PARTITION statement.
 */
public class AddPartitionsOperation extends AlterTableOperation {

	private final boolean ifNotExists;
	private final List<CatalogPartitionSpec> partitionSpecs;
	private final List<CatalogPartition> catalogPartitions;

	public AddPartitionsOperation(ObjectIdentifier tableIdentifier, boolean ifNotExists,
			List<CatalogPartitionSpec> partitionSpecs, List<CatalogPartition> catalogPartitions) {
		super(tableIdentifier);
		this.ifNotExists = ifNotExists;
		this.partitionSpecs = partitionSpecs;
		this.catalogPartitions = catalogPartitions;
	}

	public List<CatalogPartitionSpec> getPartitionSpecs() {
		return partitionSpecs;
	}

	public List<CatalogPartition> getCatalogPartitions() {
		return catalogPartitions;
	}

	public boolean ifNotExists() {
		return ifNotExists;
	}

	@Override
	public String asSummaryString() {
		StringBuilder builder = new StringBuilder(String.format("ALTER TABLE %s ADD", tableIdentifier.asSummaryString()));
		if (ifNotExists) {
			builder.append(" IF NOT EXISTS");
		}
		for (int i = 0; i < partitionSpecs.size(); i++) {
			String spec = OperationUtils.formatPartitionSpec(partitionSpecs.get(i));
			builder.append(String.format(" PARTITION (%s)", spec));
			Map<String, String> properties = catalogPartitions.get(i).getProperties();
			if (!properties.isEmpty()) {
				builder.append(String.format(" WITH (%s)", OperationUtils.formatProperties(properties)));
			}
		}
		return builder.toString();
	}
}
