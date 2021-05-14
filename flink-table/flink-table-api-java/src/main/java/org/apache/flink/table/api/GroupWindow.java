package org.apache.flink.table.api;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.expressions.ApiExpressionUtils;
import org.apache.flink.table.expressions.Expression;

/**
 * A group window specification.
 *
 * <p>Group windows group rows based on time or row-count intervals and is therefore essentially a
 * special type of groupBy. Just like groupBy, group windows allow to compute aggregates
 * on groups of elements.
 *
 * <p>Infinite streaming tables can only be grouped into time or row intervals. Hence window
 * grouping is required to apply aggregations on streaming tables.
 *
 * <p>For finite batch tables, group windows provide shortcuts for time-based groupBy.
 */
@PublicEvolving
public abstract class GroupWindow {

	/** Alias name for the group window. */
	private final Expression alias;
	private final Expression timeField;

	GroupWindow(Expression alias, Expression timeField) {
		this.alias = ApiExpressionUtils.unwrapFromApi(alias);
		this.timeField = ApiExpressionUtils.unwrapFromApi(timeField);
	}

	public Expression getAlias() {
		return alias;
	}

	public Expression getTimeField() {
		return timeField;
	}
}
