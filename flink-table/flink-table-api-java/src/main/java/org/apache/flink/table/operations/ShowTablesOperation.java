package org.apache.flink.table.operations;

/**
 * Operation to describe a SHOW TABLES statement.
 */
public class ShowTablesOperation implements ShowOperation {

	@Override
	public String asSummaryString() {
		return "SHOW TABLES";
	}
}
