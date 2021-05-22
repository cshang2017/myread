
package org.apache.flink.runtime.executiongraph;

/**
 * Base class for exceptions occurring in the {@link ExecutionGraph}.
 */
public class ExecutionGraphException extends Exception {


	public ExecutionGraphException(String message) {
		super(message);
	}

	public ExecutionGraphException(String message, Throwable cause) {
		super(message, cause);
	}

	public ExecutionGraphException(Throwable cause) {
		super(cause);
	}
}
