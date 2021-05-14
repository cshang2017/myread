

package org.apache.flink.runtime.execution;

/**
 * Thrown to trigger a canceling of the executing task. Intended to cause a cancelled status, rather
 * than a failed status.
 */
public class CancelTaskException extends RuntimeException {

	private static final long serialVersionUID = 1L;

	public CancelTaskException(Throwable cause) {
		super(cause);
	}

	public CancelTaskException(String msg) {
		super(msg);
	}

	public CancelTaskException() {
		super();
	}
}
