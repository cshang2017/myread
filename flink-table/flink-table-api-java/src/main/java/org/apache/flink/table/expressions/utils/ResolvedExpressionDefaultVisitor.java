

package org.apache.flink.table.expressions.utils;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.expressions.LocalReferenceExpression;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.expressions.ResolvedExpressionVisitor;
import org.apache.flink.table.expressions.TableReferenceExpression;
import org.apache.flink.table.expressions.TypeLiteralExpression;
import org.apache.flink.table.expressions.ValueLiteralExpression;

/**
 * A utility {@link ResolvedExpressionVisitor} that calls {@link #defaultMethod(ResolvedExpression)}
 * by default, unless other methods are overridden explicitly.
 */
@Internal
public abstract class ResolvedExpressionDefaultVisitor<T> extends ResolvedExpressionVisitor<T> {

	@Override
	public T visit(TableReferenceExpression tableReference) {
		return defaultMethod(tableReference);
	}

	@Override
	public T visit(LocalReferenceExpression localReference) {
		return defaultMethod(localReference);
	}

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

	protected abstract T defaultMethod(ResolvedExpression expression);
}
