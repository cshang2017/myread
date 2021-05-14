
package org.apache.flink.table.expressions;

import org.apache.flink.annotation.Internal;

/**
 * Implementation of {@link ExpressionVisitor} that redirects all calls to {@link #defaultMethod(Expression)}.
 */
@Internal
public abstract class ExpressionDefaultVisitor<T> implements ExpressionVisitor<T> {

	@Override
	public T visit(CallExpression call) {
		return defaultMethod(call);
	}

	@Override
	public T visit(ValueLiteralExpression valueLiteral) {
		return defaultMethod(valueLiteral);
	}

	@Override
	public T visit(FieldReferenceExpression fieldReference) {
		return defaultMethod(fieldReference);
	}

	@Override
	public T visit(TypeLiteralExpression typeLiteral) {
		return defaultMethod(typeLiteral);
	}

	@Override
	public T visit(Expression other) {
		return defaultMethod(other);
	}

	protected abstract T defaultMethod(Expression expression);
}
