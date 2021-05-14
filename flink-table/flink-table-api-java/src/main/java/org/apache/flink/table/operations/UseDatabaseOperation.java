

package org.apache.flink.table.operations;

/**
 * Operation to describe a USE [catalogName.]dataBaseName statement.
 */
public class UseDatabaseOperation implements UseOperation {

	private final String catalogName;
	private final String databaseName;

	public UseDatabaseOperation(String catalogName, String databaseName) {
		this.catalogName = catalogName;
		this.databaseName = databaseName;
	}

	public String getCatalogName() {
		return catalogName;
	}

	public String getDatabaseName() {
		return databaseName;
	}

	@Override
	public String asSummaryString() {
		return String.format("USE %s.%s", catalogName, databaseName);
	}
}
