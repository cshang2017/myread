package org.apache.flink.table.planner.plan;

/**
 * Enumerations for partial final aggregate types.
 *
 * @see org.apache.flink.table.planner.plan.rules.logical.SplitAggregateRule
 */
public enum PartialFinalType {
	/**
	 * partial aggregate type represents partial-aggregation,
	 * which produces a partial distinct aggregated result based on group key and bucket number.
	 */
	PARTIAL,
	/**
	 * final aggregate type represents final-aggregation,
	 * which produces final result based on the partially distinct aggregated result.
	 */
	FINAL,
	/**
	 * the aggregate which has not been split.
	 */
	NONE
}
