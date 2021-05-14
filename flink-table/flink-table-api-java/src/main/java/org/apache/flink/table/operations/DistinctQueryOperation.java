

package org.apache.flink.table.operations;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.TableSchema;

import java.util.Collections;
import java.util.List;

/**
 * Removes duplicated rows of underlying relational operation.
 */
@Internal
public class DistinctQueryOperation implements QueryOperation {

	private final QueryOperation child;

	public DistinctQueryOperation(QueryOperation child) {
		this.child = child;
	}

	@Override
	public TableSchema getTableSchema() {
		return child.getTableSchema();
	}

	@Override
	public String asSummaryString() {
		return OperationUtils.formatWithChildren(
			"Distinct",
			Collections.emptyMap(),
			getChildren(),
			Operation::asSummaryString);
	}

	@Override
	public List<QueryOperation> getChildren() {
		return Collections.singletonList(child);
	}

	@Override
	public <T> T accept(QueryOperationVisitor<T> visitor) {
		return visitor.visit(this);
	}
}
