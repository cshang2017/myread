

package org.apache.flink.table.expressions;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.TableException;

/**
 * A visitor for all {@link ResolvedExpression}s.
 *
 * <p>All expressions of this visitor are the output of the API and might be passed to a planner.
 */
@Internal
public abstract class ResolvedExpressionVisitor<R> implements ExpressionVisitor<R> {

	public final R visit(Expression other) {
		if (other instanceof TableReferenceExpression) {
			return visit((TableReferenceExpression) other);
		} else if (other instanceof LocalReferenceExpression) {
			return visit((LocalReferenceExpression) other);
		}
		throw new TableException("Unexpected unresolved expression received: " + other);
	}

	public abstract R visit(TableReferenceExpression tableReference);

	public abstract R visit(LocalReferenceExpression localReference);
}
