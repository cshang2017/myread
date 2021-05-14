

package org.apache.flink.table.expressions;

import org.apache.flink.annotation.Internal;

import java.util.Optional;

/**
 * Utility methods for working with {@link Expression}s.
 */
@Internal
public final class ExpressionUtils {

	/**
	 * Extracts the value (excluding null) of a given class from an expression assuming it is a
	 * {@link ValueLiteralExpression}.
	 *
	 * @param expression literal to extract the value from
	 * @param targetClass expected class to extract from the literal
	 * @param <V> type of extracted value
	 * @return extracted value or empty if could not extract value of given type
	 */
	public static <V> Optional<V> extractValue(Expression expression, Class<V> targetClass) {
		if (expression instanceof ValueLiteralExpression) {
			final ValueLiteralExpression valueLiteral = (ValueLiteralExpression) expression;
			return valueLiteral.getValueAs(targetClass);
		}
		return Optional.empty();
	}

	private ExpressionUtils() {
	}
}
