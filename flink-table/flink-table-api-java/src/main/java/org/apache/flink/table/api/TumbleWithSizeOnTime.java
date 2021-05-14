package org.apache.flink.table.api;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.expressions.ApiExpressionUtils;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.ExpressionParser;

/**
 * Tumbling window on time.
 */
@PublicEvolving
public final class TumbleWithSizeOnTime {

	private final Expression time;
	private final Expression size;

	TumbleWithSizeOnTime(Expression time, Expression size) {
		this.time = ApiExpressionUtils.unwrapFromApi(time);
		this.size = ApiExpressionUtils.unwrapFromApi(size);
	}

	/**
	 * Assigns an alias for this window that the following {@code groupBy()} and {@code select()}
	 * clause can refer to. {@code select()} statement can access window properties such as window
	 * start or end time.
	 *
	 * @param alias alias for this window
	 * @return this window
	 */
	public TumbleWithSizeOnTimeWithAlias as(Expression alias) {
		return new TumbleWithSizeOnTimeWithAlias(alias, time, size);
	}

	/**
	 * Assigns an alias for this window that the following {@code groupBy()} and {@code select()}
	 * clause can refer to. {@code select()} statement can access window properties such as window
	 * start or end time.
	 *
	 * @param alias alias for this window
	 * @return this window
	 */
	public TumbleWithSizeOnTimeWithAlias as(String alias) {
		return as(ExpressionParser.parseExpression(alias));
	}
}
