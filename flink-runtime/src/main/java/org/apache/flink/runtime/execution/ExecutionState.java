package org.apache.flink.runtime.execution;

/**
 * An enumeration of all states that a task can be in during its execution.
 * Tasks usually start in the state {@code CREATED} and switch states according to
 * this diagram:
 * <pre>{@code
 *
 *     CREATED  -> SCHEDULED -> DEPLOYING -> RUNNING -> FINISHED
 *        |            |            |          |
 *        |            |            |   +------+
 *        |            |            V   V
 *        |            |         CANCELLING -----+----> CANCELED
 *        |            |                         |
 *        |            +-------------------------+
 *        |
 *        |                                   ... -> FAILED
 *        V
 *    RECONCILING  -> RUNNING | FINISHED | CANCELED | FAILED
 *
 * }</pre>
 *
 * <p>It is possible to enter the {@code RECONCILING} state from {@code CREATED}
 * state if job manager fail over, and the {@code RECONCILING} state can switch into
 * any existing task state.
 *
 * <p>It is possible to enter the {@code FAILED} state from any other state.
 *
 * <p>The states {@code FINISHED}, {@code CANCELED}, and {@code FAILED} are
 * considered terminal states.
 */
public enum ExecutionState {

	CREATED,
	
	SCHEDULED,
	
	DEPLOYING,
	
	RUNNING,

	/**
	 * This state marks "successfully completed". It can only be reached when a
	 * program reaches the "end of its input". The "end of input" can be reached
	 * when consuming a bounded input (fix set of files, bounded query, etc) or
	 * when stopping a program (not cancelling!) which make the input look like
	 * it reached its end at a specific point.
	 */
	FINISHED,
	
	CANCELING,
	
	CANCELED,
	
	FAILED,

	RECONCILING;

	public boolean isTerminal() {
		return this == FINISHED || this == CANCELED || this == FAILED;
	}
}
