
package org.apache.flink.table.operations.ddl;

import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.OperationUtils;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Operation to describe a CREATE TABLE statement.
 */
public class CreateTableOperation implements CreateOperation {
	private final ObjectIdentifier tableIdentifier;
	private CatalogTable catalogTable;
	private boolean ignoreIfExists;
	private boolean isTemporary;

	public CreateTableOperation(
			ObjectIdentifier tableIdentifier,
			CatalogTable catalogTable,
			boolean ignoreIfExists,
			boolean isTemporary) {
		this.tableIdentifier = tableIdentifier;
		this.catalogTable = catalogTable;
		this.ignoreIfExists = ignoreIfExists;
		this.isTemporary = isTemporary;
	}

	public CatalogTable getCatalogTable() {
		return catalogTable;
	}

	public ObjectIdentifier getTableIdentifier() {
		return tableIdentifier;
	}

	public boolean isIgnoreIfExists() {
		return ignoreIfExists;
	}

	public boolean isTemporary() {
		return isTemporary;
	}

	@Override
	public String asSummaryString() {
		Map<String, Object> params = new LinkedHashMap<>();
		params.put("catalogTable", catalogTable.toProperties());
		params.put("identifier", tableIdentifier);
		params.put("ignoreIfExists", ignoreIfExists);
		params.put("isTemporary", isTemporary);

		return OperationUtils.formatWithChildren(
			"CREATE TABLE",
			params,
			Collections.emptyList(),
			Operation::asSummaryString);
	}
}
