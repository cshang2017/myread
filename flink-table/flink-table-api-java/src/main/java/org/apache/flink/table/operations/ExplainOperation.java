
package org.apache.flink.table.operations;

import java.util.Collections;

/**
 * Operation to describe an EXPLAIN statement.
 * NOTES: currently, only default behavior(EXPLAIN PLAN FOR xx) is supported.
 */
public class ExplainOperation implements Operation {
	private final Operation child;

	public ExplainOperation(Operation child) {
		this.child = child;
	}

	public Operation getChild() {
		return child;
	}

	@Override
	public String asSummaryString() {
		return OperationUtils.formatWithChildren(
				"EXPLAIN PLAN FOR",
				Collections.emptyMap(),
				Collections.singletonList(child),
				Operation::asSummaryString);
	}
}
