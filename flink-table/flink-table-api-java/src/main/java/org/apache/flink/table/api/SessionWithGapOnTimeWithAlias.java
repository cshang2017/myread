package org.apache.flink.table.api;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.expressions.ApiExpressionUtils;
import org.apache.flink.table.expressions.Expression;

/**
 * Session window on time with alias. Fully specifies a window.
 */
@PublicEvolving
public final class SessionWithGapOnTimeWithAlias extends GroupWindow {

	private final Expression gap;

	SessionWithGapOnTimeWithAlias(Expression alias, Expression timeField, Expression gap) {
		super(alias, timeField);
		this.gap = ApiExpressionUtils.unwrapFromApi(gap);
	}

	public Expression getGap() {
		return gap;
	}
}
