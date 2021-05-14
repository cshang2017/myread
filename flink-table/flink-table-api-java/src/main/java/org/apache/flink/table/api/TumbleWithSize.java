package org.apache.flink.table.api;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.expressions.ApiExpressionUtils;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.ExpressionParser;

/**
 * Tumbling window.
 *
 * <p>For streaming tables you can specify grouping by a event-time or processing-time attribute.
 *
 * <p>For batch tables you can specify grouping on a timestamp or long attribute.
 */
@PublicEvolving
public final class TumbleWithSize {

	/** The size of the window either as time or row-count interval. */
	private Expression size;

	TumbleWithSize(Expression size) {
		this.size = ApiExpressionUtils.unwrapFromApi(size);
	}

	/**
	 * Specifies the time attribute on which rows are grouped.
	 *
	 * <p>For streaming tables you can specify grouping by a event-time or processing-time
	 * attribute.
	 *
	 * <p>For batch tables you can specify grouping on a timestamp or long attribute.
	 *
	 * @param timeField time attribute for streaming and batch tables
	 * @return a tumbling window on event-time
	 */
	public TumbleWithSizeOnTime on(Expression timeField) {
		return new TumbleWithSizeOnTime(timeField, size);
	}

	/**
	 * Specifies the time attribute on which rows are grouped.
	 *
	 * <p>For streaming tables you can specify grouping by a event-time or processing-time
	 * attribute.
	 *
	 * <p>For batch tables you can specify grouping on a timestamp or long attribute.
	 *
	 * @param timeField time attribute for streaming and batch tables
	 * @return a tumbling window on event-time
	 * @deprecated use {@link #on(Expression)}
	 */
	@Deprecated
	public TumbleWithSizeOnTime on(String timeField) {
		return on(ExpressionParser.parseExpression(timeField));
	}
}
