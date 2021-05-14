package org.apache.flink.table.operations;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.expressions.ResolvedExpression;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Relational operation that performs computations on top of subsets of input rows grouped by
 * key.
 */
@Internal
public class AggregateQueryOperation implements QueryOperation {

	private final List<ResolvedExpression> groupingExpressions;
	private final List<ResolvedExpression> aggregateExpressions;
	private final QueryOperation child;
	private final TableSchema tableSchema;

	public AggregateQueryOperation(
			List<ResolvedExpression> groupingExpressions,
			List<ResolvedExpression> aggregateExpressions,
			QueryOperation child,
			TableSchema tableSchema) {
		this.groupingExpressions = groupingExpressions;
		this.aggregateExpressions = aggregateExpressions;
		this.child = child;
		this.tableSchema = tableSchema;
	}

	@Override
	public TableSchema getTableSchema() {
		return tableSchema;
	}

	@Override
	public String asSummaryString() {
		Map<String, Object> args = new LinkedHashMap<>();
		args.put("group", groupingExpressions);
		args.put("agg", aggregateExpressions);

		return OperationUtils.formatWithChildren("Aggregate", args, getChildren(), Operation::asSummaryString);
	}

	public List<ResolvedExpression> getGroupingExpressions() {
		return groupingExpressions;
	}

	public List<ResolvedExpression> getAggregateExpressions() {
		return aggregateExpressions;
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
