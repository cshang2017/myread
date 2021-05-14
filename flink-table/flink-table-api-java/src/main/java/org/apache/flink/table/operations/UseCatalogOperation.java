package org.apache.flink.table.operations;

/**
 * Operation to describe a USE CATALOG statement.
 */
public class UseCatalogOperation implements UseOperation {

	private String catalogName;

	public UseCatalogOperation(String catalogName) {
		this.catalogName = catalogName;
	}

	public String getCatalogName() {
		return catalogName;
	}

	@Override
	public String asSummaryString() {
		return String.format("USE CATALOGS %s", catalogName);
	}
}
