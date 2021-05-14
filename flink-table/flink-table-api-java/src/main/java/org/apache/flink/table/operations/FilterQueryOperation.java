
package org.apache.flink.table.operations;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.expressions.ResolvedExpression;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Filters out rows of underlying relational operation that do not match given condition.
 */
@Internal
public class FilterQueryOperation implements QueryOperation {

	private final ResolvedExpression condition;
	private final QueryOperation child;

	public FilterQueryOperation(ResolvedExpression condition, QueryOperation child) {
		this.condition = condition;
		this.child = child;
	}

	public ResolvedExpression getCondition() {
		return condition;
	}

	@Override
	public TableSchema getTableSchema() {
		return child.getTableSchema();
	}

	@Override
	public String asSummaryString() {
		Map<String, Object> args = new LinkedHashMap<>();
		args.put("condition", condition);

		return OperationUtils.formatWithChildren("Filter", args, getChildren(), Operation::asSummaryString);
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
