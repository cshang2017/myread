package org.apache.flink.runtime.executiongraph;

import org.apache.flink.runtime.execution.ExecutionState;

/**
 * A special {@link IllegalStateException} indicating a mismatch in the expected and actual
 * {@link ExecutionState} of an {@link Execution}.
 */
public class IllegalExecutionStateException extends IllegalStateException {

	private static final long serialVersionUID = 1L;

	/**
	 * Creates a new IllegalExecutionStateException with the error message indicating
	 * the expected and actual state.
	 * 
	 * @param expected The expected state 
	 * @param actual   The actual state
	 */
	public IllegalExecutionStateException(ExecutionState expected, ExecutionState actual) {
		super("Invalid execution state: Expected " + expected + " , found " + actual);
	}

	/**
	 * Creates a new IllegalExecutionStateException with the error message indicating
	 * the expected and actual state.
	 *
	 * @param expected The expected state 
	 * @param actual   The actual state
	 */
	public IllegalExecutionStateException(Execution execution, ExecutionState expected, ExecutionState actual) {
		super(execution.getVertexWithAttempt() + " is no longer in expected state " + expected + 
				" but in state " + actual);
	}
}
