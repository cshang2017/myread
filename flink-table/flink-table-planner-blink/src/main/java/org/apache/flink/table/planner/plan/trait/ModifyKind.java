package org.apache.flink.table.planner.plan.trait;

/**
 * Lists all kinds of modify operations that happen in a changelog.
 */
public enum ModifyKind {
	/**
	 * Insertion operation.
	 */
	INSERT,

	/**
	 * Update operation.
	 */
	UPDATE,

	/**
	 * Deletion operation.
	 */
	DELETE
}
