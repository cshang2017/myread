package org.apache.flink.runtime.execution;

import org.apache.flink.runtime.throwable.ThrowableAnnotation;
import org.apache.flink.runtime.throwable.ThrowableType;

/**
 * Exception thrown in order to suppress job restarts.
 *
 * <p>This exception acts as a wrapper around the real cause and suppresses
 * job restarts. The JobManager will <strong>not</strong> restart a job, which
 * fails with this Exception.
 */
@ThrowableAnnotation(ThrowableType.NonRecoverableError)
public class SuppressRestartsException extends RuntimeException {

	public SuppressRestartsException(Throwable cause) {
		super("Unrecoverable failure. This suppresses job restarts. Please check the " +
				"stack trace for the root cause.", cause);
	}

}
