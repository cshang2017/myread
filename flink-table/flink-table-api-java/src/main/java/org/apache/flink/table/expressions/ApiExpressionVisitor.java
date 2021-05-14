package org.apache.flink.table.expressions;

import org.apache.flink.annotation.Internal;

/**
 * A visitor for all {@link Expression}s that might be created during API translation.
 */
@Internal
public abstract class ApiExpressionVisitor<R> implements ExpressionVisitor<R> {

	public final R visit(Expression other) {
		if (other instanceof UnresolvedReferenceExpression) {
			return visit((UnresolvedReferenceExpression) other);
		} else if (other instanceof TableReferenceExpression) {
			return visit((TableReferenceExpression) other);
		} else if (other instanceof LocalReferenceExpression) {
			return visit((LocalReferenceExpression) other);
		} else if (other instanceof LookupCallExpression) {
			return visit((LookupCallExpression) other);
		} else if (other instanceof UnresolvedCallExpression) {
			return visit((UnresolvedCallExpression) other);
		}
		return visitNonApiExpression(other);
	}

	// --------------------------------------------------------------------------------------------
	// resolved API expressions
	// --------------------------------------------------------------------------------------------

	public abstract R visit(TableReferenceExpression tableReference);

	public abstract R visit(LocalReferenceExpression localReference);

	// --------------------------------------------------------------------------------------------
	// unresolved API expressions
	// --------------------------------------------------------------------------------------------

	public abstract R visit(UnresolvedReferenceExpression unresolvedReference);

	public abstract R visit(LookupCallExpression lookupCall);

	public abstract R visit(UnresolvedCallExpression unresolvedCallExpression);

	// --------------------------------------------------------------------------------------------
	// other expressions
	// --------------------------------------------------------------------------------------------

	public abstract R visitNonApiExpression(Expression other);
}
