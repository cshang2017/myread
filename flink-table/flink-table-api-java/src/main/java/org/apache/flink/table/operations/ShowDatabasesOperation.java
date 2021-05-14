package org.apache.flink.table.operations;

/**
 * Operation to describe a SHOW DATABASES statement.
 */
public class ShowDatabasesOperation implements ShowOperation {

	@Override
	public String asSummaryString() {
		return "SHOW DATABASES";
	}
}
