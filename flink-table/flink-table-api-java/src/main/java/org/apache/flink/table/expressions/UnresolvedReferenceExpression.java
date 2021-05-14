
package org.apache.flink.table.expressions;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.util.Preconditions;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * An unresolved reference to a field, table, or local reference.
 *
 * <p>This is a purely API facing expression that will be resolved into
 * {@link FieldReferenceExpression}, {@link LocalReferenceExpression},
 * or {@link TableReferenceExpression}.
 */
@PublicEvolving
public final class UnresolvedReferenceExpression implements Expression {

	private final String name;

	UnresolvedReferenceExpression(String name) {
		this.name = Preconditions.checkNotNull(name);
	}

	public String getName() {
		return name;
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
