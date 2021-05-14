package org.apache.flink.table.operations;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.ResolvedExpression;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Table operation that computes new table using given {@link Expression}s
 * from its input relational operation.
 */
@Internal
public class ProjectQueryOperation implements QueryOperation {

	private final List<ResolvedExpression> projectList;
	private final QueryOperation child;
	private final TableSchema tableSchema;

	public ProjectQueryOperation(
			List<ResolvedExpression> projectList,
			QueryOperation child,
			TableSchema tableSchema) {
		this.projectList = projectList;
		this.child = child;
		this.tableSchema = tableSchema;
	}

	public List<ResolvedExpression> getProjectList() {
		return projectList;
	}

	@Override
	public TableSchema getTableSchema() {
		return tableSchema;
	}

	@Override
	public String asSummaryString() {
		Map<String, Object> args = new LinkedHashMap<>();
		args.put("projections", projectList);

		return OperationUtils.formatWithChildren("Project", args, getChildren(), Operation::asSummaryString);
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
