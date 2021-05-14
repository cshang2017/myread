package org.apache.flink.runtime.taskexecutor;

/**
 * Different file types to request from the {@link TaskExecutor}.
 */
public enum FileType {
	/**
	 * The log file type for taskmanager.
	 */
	LOG,

	/**
	 * The stdout file type for taskmanager.
	 */
	STDOUT,
}
