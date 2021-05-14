package org.apache.flink.table.operations;

/**
 * Operation to describe a SHOW CATALOGS statement.
 */
public class ShowCatalogsOperation implements ShowOperation {

	@Override
	public String asSummaryString() {
		return "SHOW CATALOGS";
	}
}
