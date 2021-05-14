package org.apache.flink.table.expressions;

import org.apache.flink.annotation.PublicEvolving;

/**
 * The visitor definition of {@link Expression}.
 *
 * <p>An expression visitor transforms an expression to instances of {@code R}.
 *
 * <p>Please note that only {@link ResolvedExpression}s are listed here. Pure API expression are handled
 * in {@link #visit(Expression)}.
 */
@PublicEvolving
public interface ExpressionVisitor<R> {

	// --------------------------------------------------------------------------------------------
	// resolved expressions
	// --------------------------------------------------------------------------------------------

	R visit(CallExpression call);

	R visit(ValueLiteralExpression valueLiteral);

	R visit(FieldReferenceExpression fieldReference);

	R visit(TypeLiteralExpression typeLiteral);

	// --------------------------------------------------------------------------------------------
	// other expressions
	// --------------------------------------------------------------------------------------------

	R visit(Expression other);
}
