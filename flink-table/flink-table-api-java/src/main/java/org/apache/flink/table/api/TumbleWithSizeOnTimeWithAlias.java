package org.apache.flink.table.api;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.expressions.ApiExpressionUtils;
import org.apache.flink.table.expressions.Expression;

/**
 * Tumbling window on time with alias. Fully specifies a window.
 */
@PublicEvolving
public final class TumbleWithSizeOnTimeWithAlias extends GroupWindow {

	private final Expression size;

	TumbleWithSizeOnTimeWithAlias(Expression alias, Expression timeField, Expression size) {
		super(alias, timeField);
		this.size = ApiExpressionUtils.unwrapFromApi(size);
	}

	public Expression getSize() {
		return size;
	}
}
