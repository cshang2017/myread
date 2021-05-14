package org.apache.flink.table.api;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.ExpressionParser;

/**
 * Helper class for creating a session window. The boundary of session windows are defined by
 * intervals of inactivity, i.e., a session window is closes if no event appears for a defined
 * gap period.
 *
 * <p>Java Example:
 *
 * <pre>
 * {@code
 *    Session.withGap("10.minutes").on("rowtime").as("w")
 * }
 * </pre>
 *
 * <p>Scala Example:
 *
 * <pre>
 * {@code
 *    Session withGap 10.minutes on 'rowtime as 'w
 * }
 * </pre>
 */
@PublicEvolving
public final class Session {

	/**
	 * Creates a session window. The boundary of session windows are defined by
	 * intervals of inactivity, i.e., a session window is closes if no event appears for a defined
	 * gap period.
	 *
	 * @param gap specifies how long (as interval of milliseconds) to wait for new data before
	 *            closing the session window.
	 * @return a partially defined session window
	 * @deprecated use {@link #withGap(Expression)}
	 */
	@Deprecated
	public static SessionWithGap withGap(String gap) {
		return withGap(ExpressionParser.parseExpression(gap));
	}

	/**
	 * Creates a session window. The boundary of session windows are defined by
	 * intervals of inactivity, i.e., a session window is closes if no event appears for a defined
	 * gap period.
	 *
	 * @param gap specifies how long (as interval of milliseconds) to wait for new data before
	 *            closing the session window.
	 * @return a partially defined session window
	 */
	public static SessionWithGap withGap(Expression gap) {
		return new SessionWithGap(gap);
	}
}
