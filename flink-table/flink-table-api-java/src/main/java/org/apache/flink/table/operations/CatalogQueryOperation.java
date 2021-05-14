

package org.apache.flink.table.operations;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.ObjectIdentifier;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Describes a relational operation that was created from a lookup to a catalog.
 */
@Internal
public class CatalogQueryOperation implements QueryOperation {

	private final ObjectIdentifier tableIdentifier;
	private final TableSchema tableSchema;

	public CatalogQueryOperation(ObjectIdentifier tableIdentifier, TableSchema tableSchema) {
		this.tableIdentifier = tableIdentifier;
		this.tableSchema = tableSchema;
	}

	public ObjectIdentifier getTableIdentifier() {
		return tableIdentifier;
	}

	@Override
	public TableSchema getTableSchema() {
		return tableSchema;
	}

	@Override
	public String asSummaryString() {
		Map<String, Object> args = new LinkedHashMap<>();
		args.put("identifier", tableIdentifier);
		args.put("fields", tableSchema.getFieldNames());

		return OperationUtils.formatWithChildren("CatalogTable", args, getChildren(), Operation::asSummaryString);
	}

	@Override
	public List<QueryOperation> getChildren() {
		return Collections.emptyList();
	}

	@Override
	public <T> T accept(QueryOperationVisitor<T> visitor) {
		return visitor.visit(this);
	}
}
