package org.apache.flink.table.expressions;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.types.DataType;

import java.util.List;

/**
 * General interface for all kinds of expressions.
 *
 * <p>Expressions represent a logical tree for producing a computation result. Every expression
 * consists of zero, one, or more subexpressions. Expressions might be literal values, function calls,
 * or field references.
 *
 * <p>Expressions are part of the API. They might be transformed multiple times within the API stack
 * until they are fully {@link ResolvedExpression}s. Value types and output types are expressed as
 * instances of {@link DataType}.
 */
@PublicEvolving
public interface Expression {

	/**
	 * Returns a string that summarizes this expression for printing to a console. An implementation
	 * might skip very specific properties.
	 *
	 * @return summary string of this expression for debugging purposes
	 */
	String asSummaryString();

	List<Expression> getChildren();

	<R> R accept(ExpressionVisitor<R> visitor);
}
