package org.apache.flink.runtime.jobmanager.scheduler;

import org.apache.flink.runtime.JobException;

/**
 * Indicates resource allocation failures.
 */
public class NoResourceAvailableException extends JobException {

	private static final String BASE_MESSAGE = "Not enough free slots available to run the job. "
		+ "You can decrease the operator parallelism or increase the number of slots per TaskManager in the configuration.";

	public NoResourceAvailableException() {
		super(BASE_MESSAGE);
	}

	public NoResourceAvailableException(String message) {
		super(message);
	}

	public NoResourceAvailableException(String message, Throwable cause) {
		super(message, cause);
	}

}
