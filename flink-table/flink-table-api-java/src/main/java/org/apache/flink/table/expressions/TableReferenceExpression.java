package org.apache.flink.table.expressions;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.operations.QueryOperation;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.Preconditions;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Expression that references another table.
 *
 * <p>This is a pure API expression that is translated into uncorrelated sub-queries by the planner.
 */
@PublicEvolving
public final class TableReferenceExpression implements ResolvedExpression {

	private final String name;
	private final QueryOperation queryOperation;

	TableReferenceExpression(String name, QueryOperation queryOperation) {
		this.name = Preconditions.checkNotNull(name);
		this.queryOperation = Preconditions.checkNotNull(queryOperation);
	}

	public String getName() {
		return name;
	}

	public QueryOperation getQueryOperation() {
		return queryOperation;
	}

	@Override
	public DataType getOutputDataType() {
		return queryOperation.getTableSchema().toRowDataType();
	}

	@Override
	public List<ResolvedExpression> getResolvedChildren() {
		return Collections.emptyList();
	}

	@Override
	public String asSummaryString() {
		return name;
	}

	@Override
	public List<Expression> getChildren() {
		return Collections.emptyList();
	}

	@Override
	public <R> R accept(ExpressionVisitor<R> visitor) {
		return visitor.visit(this);
	}



	@Override
	public String toString() {
		return asSummaryString();
	}
}
