package org.apache.flink.runtime.jobmaster;

import org.apache.flink.runtime.executiongraph.ExecutionGraph;

/**
 * Exceptions thrown by operations on the {@link ExecutionGraph} by the {@link JobMaster}.
 */
public class ExecutionGraphException extends JobManagerException {

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
