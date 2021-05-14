package org.apache.flink.table.api;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.ExpressionParser;

/**
 * Helper class for creating a tumbling window. Tumbling windows are consecutive, non-overlapping
 * windows of a specified fixed length. For example, a tumbling window of 5 minutes size groups
 * elements in 5 minutes intervals.
 *
 * <p>Java Example:
 *
 * <pre>
 * {@code
 *    Tumble.over("10.minutes").on("rowtime").as("w")
 * }
 * </pre>
 *
 * <p>Scala Example:
 *
 * <pre>
 * {@code
 *    Tumble over 5.minutes on 'rowtime as 'w
 * }
 * </pre>
 */
@PublicEvolving
public final class Tumble {

	/**
	 * Creates a tumbling window. Tumbling windows are fixed-size, consecutive, non-overlapping
	 * windows of a specified fixed length. For example, a tumbling window of 5 minutes size groups
	 * elements in 5 minutes intervals.
	 *
	 * @param size the size of the window as time or row-count interval.
	 * @return a partially defined tumbling window
	 * @deprecated use {@link #over(Expression)}
	 */
	@Deprecated
	public static TumbleWithSize over(String size) {
		return over(ExpressionParser.parseExpression(size));
	}

	/**
	 * Creates a tumbling window. Tumbling windows are fixed-size, consecutive, non-overlapping
	 * windows of a specified fixed length. For example, a tumbling window of 5 minutes size groups
	 * elements in 5 minutes intervals.
	 *
	 * @param size the size of the window as time or row-count interval.
	 * @return a partially defined tumbling window
	 */
	public static TumbleWithSize over(Expression size) {
		return new TumbleWithSize(size);
	}
}
