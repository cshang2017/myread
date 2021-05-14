package org.apache.flink.table.api;

import org.apache.flink.table.api.internal.BaseExpressions;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.ExpressionVisitor;

import java.util.List;

/**
 * Java API class that gives access to expression operations.
 *
 * @see BaseExpressions
 */
public final class ApiExpression extends BaseExpressions<Object, ApiExpression> implements Expression {
	private final Expression wrappedExpression;

	@Override
	public String asSummaryString() {
		return wrappedExpression.asSummaryString();
	}

	ApiExpression(Expression wrappedExpression) {
		if (wrappedExpression instanceof ApiExpression) {
			throw new UnsupportedOperationException("This is a bug. Please file an issue.");
		}
		this.wrappedExpression = wrappedExpression;
	}

	@Override
	public Expression toExpr() {
		return wrappedExpression;
	}

	@Override
	protected ApiExpression toApiSpecificExpression(Expression expression) {
		return new ApiExpression(expression);
	}

	@Override
	public List<Expression> getChildren() {
		return wrappedExpression.getChildren();
	}

	@Override
	public <R> R accept(ExpressionVisitor<R> visitor) {
		return wrappedExpression.accept(visitor);
	}
}
