package org.apache.flink.runtime;

import org.apache.flink.util.FlinkException;

/**
 * Indicates that a job has failed.
 */
public class JobException extends FlinkException {

	public JobException(String msg) {
		super(msg);
	}

	public JobException(String message, Throwable cause) {
		super(message, cause);
	}
}
