

package org.apache.flink.table.operations;

/**
 * Operation to describe a SHOW VIEWS statement.
 */
public class ShowViewsOperation implements ShowOperation {

	@Override
	public String asSummaryString() {
		return "SHOW VIEWS";
	}
}
