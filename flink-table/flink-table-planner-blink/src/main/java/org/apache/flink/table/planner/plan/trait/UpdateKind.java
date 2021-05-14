package org.apache.flink.table.planner.plan.trait;

/**
 * Lists all kinds of {@link ModifyKind#UPDATE} operation.
 */
public enum UpdateKind {

	/**
	 * NONE doesn't represent any kind of update operation.
	 */
	NONE,

	/**
	 * This kind indicates that operators should emit update changes just as a row of
	 * {@code RowKind#UPDATE_AFTER}.
	 */
	ONLY_UPDATE_AFTER,

	/**
	 * This kind indicates that operators should emit update changes in the way that
	 * a row of {@code RowKind#UPDATE_BEFORE} and a row of {@code RowKind#UPDATE_AFTER} together.
	 */
	BEFORE_AND_AFTER
}
