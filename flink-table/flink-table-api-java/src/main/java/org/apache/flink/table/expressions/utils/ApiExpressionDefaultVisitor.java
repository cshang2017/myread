package org.apache.flink.table.expressions.utils;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.expressions.ApiExpressionVisitor;
import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.expressions.LocalReferenceExpression;
import org.apache.flink.table.expressions.LookupCallExpression;
import org.apache.flink.table.expressions.TableReferenceExpression;
import org.apache.flink.table.expressions.TypeLiteralExpression;
import org.apache.flink.table.expressions.UnresolvedCallExpression;
import org.apache.flink.table.expressions.UnresolvedReferenceExpression;
import org.apache.flink.table.expressions.ValueLiteralExpression;

/**
 * A utility {@link ApiExpressionVisitor} that calls {@link #defaultMethod(Expression)} by default,
 * unless other methods are overridden explicitly.
 */
@Internal
public abstract class ApiExpressionDefaultVisitor<T> extends ApiExpressionVisitor<T> {

	protected abstract T defaultMethod(Expression expression);

	// --------------------------------------------------------------------------------------------
	// resolved expressions
	// --------------------------------------------------------------------------------------------

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

	// --------------------------------------------------------------------------------------------
	// resolved API expressions
	// --------------------------------------------------------------------------------------------

	@Override
	public T visit(TableReferenceExpression tableReference) {
		return defaultMethod(tableReference);
	}

	@Override
	public T visit(LocalReferenceExpression localReference) {
		return defaultMethod(localReference);
	}

	// --------------------------------------------------------------------------------------------
	// unresolved API expressions
	// --------------------------------------------------------------------------------------------

	@Override
	public T visit(UnresolvedReferenceExpression unresolvedReference) {
		return defaultMethod(unresolvedReference);
	}

	@Override
	public T visit(LookupCallExpression lookupCall) {
		return defaultMethod(lookupCall);
	}

	@Override
	public T visit(UnresolvedCallExpression unresolvedCall) {
		return defaultMethod(unresolvedCall);
	}

	// --------------------------------------------------------------------------------------------
	// other expressions
	// --------------------------------------------------------------------------------------------

	@Override
	public T visitNonApiExpression(Expression other) {
		return defaultMethod(other);
	}
}
