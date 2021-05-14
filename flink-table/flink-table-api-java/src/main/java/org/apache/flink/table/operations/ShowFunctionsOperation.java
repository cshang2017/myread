

package org.apache.flink.table.operations;

/**
 * Operation to describe a SHOW FUNCTIONS statement.
 */
public class ShowFunctionsOperation implements ShowOperation {

	@Override
	public String asSummaryString() {
		return "SHOW FUNCTIONS";
	}
}
