package org.apache.flink.table.expressions;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.Preconditions;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Expression that wraps {@link DataType} as a literal.
 *
 * <p>Expressing a type is primarily needed for casting operations. This expression simplifies the
 * {@link Expression} design as it makes {@link CallExpression} the only expression that takes
 * subexpressions.
 */
@PublicEvolving
public final class TypeLiteralExpression implements ResolvedExpression {

	private final DataType dataType;

	public TypeLiteralExpression(DataType dataType) {
		this.dataType = Preconditions.checkNotNull(dataType, "Data type must not be null.");
	}

	@Override
	public DataType getOutputDataType() {
		return dataType;
	}

	@Override
	public List<ResolvedExpression> getResolvedChildren() {
		return Collections.emptyList();
	}

	@Override
	public String asSummaryString() {
		return dataType.toString();
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
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		TypeLiteralExpression that = (TypeLiteralExpression) o;
		return dataType.equals(that.dataType);
	}

	@Override
	public int hashCode() {
		return Objects.hash(dataType);
	}

	@Override
	public String toString() {
		return asSummaryString();
	}
}
