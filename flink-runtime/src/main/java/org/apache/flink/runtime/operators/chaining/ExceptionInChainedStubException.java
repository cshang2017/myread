
package org.apache.flink.runtime.operators.chaining;

/**
 * A special exception to indicate that an exception occurred in the nested call of a chained stub.
 * The exception's only purpose is to be  identifiable as such and to carry the cause exception.
 */
public class ExceptionInChainedStubException extends RuntimeException {


	private String taskName;

	private Exception exception;

	public ExceptionInChainedStubException(String taskName, Exception wrappedException) {
		super("Exception in chained task '" + taskName + "'", exceptionUnwrap(wrappedException));
		this.taskName = taskName;
		this.exception = wrappedException;
	}

	public String getTaskName() {
		return taskName;
	}

	public Exception getWrappedException() {
		return exception;
	}

	public static Exception exceptionUnwrap(Exception e) {
		if (e instanceof ExceptionInChainedStubException) {
			return exceptionUnwrap(((ExceptionInChainedStubException) e).getWrappedException());
		}

		return e;
	}
}
