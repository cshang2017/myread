package org.apache.flink.runtime.executiongraph;

import org.apache.flink.util.Preconditions;
import org.apache.flink.util.SerializedThrowable;

import java.io.Serializable;

/**
 * Simple container to hold an exception and the corresponding timestamp.
 */
public class ErrorInfo implements Serializable {

	/** The exception that we keep holding forever. Has no strong reference to any user-defined code. */
	private final SerializedThrowable exception;

	private final long timestamp;
	public ErrorInfo(Throwable exception, long timestamp) {
		Preconditions.checkNotNull(exception);
		Preconditions.checkArgument(timestamp > 0);

		this.exception = exception instanceof SerializedThrowable ?
				(SerializedThrowable) exception : new SerializedThrowable(exception);
		this.timestamp = timestamp;
	}

	/**
	 * Returns the serialized form of the original exception.
	 */
	public SerializedThrowable getException() {
		return exception;
	}

	/**
	 * Returns the contained exception as a string.
	 *
	 * @return failure causing exception as a string, or {@code "(null)"}
	 */
	public String getExceptionAsString() {
		return exception.getFullStringifiedStackTrace();
	}

	/**
	 * Returns the timestamp for the contained exception.
	 *
	 * @return timestamp of contained exception, or 0 if no exception was set
	 */
	public long getTimestamp() {
		return timestamp;
	}
}
