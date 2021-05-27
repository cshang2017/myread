package org.apache.flink.runtime.operators.coordination;

import org.apache.flink.util.FlinkException;

/**
 * An exception indicating that a target task is not running.
 */
public class TaskNotRunningException extends FlinkException {

	private static final long serialVersionUID = 1L;

	public TaskNotRunningException(String message) {
		super(message);
	}
}
