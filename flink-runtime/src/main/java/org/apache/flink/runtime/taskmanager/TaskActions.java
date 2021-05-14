package org.apache.flink.runtime.taskmanager;

/**
 * Actions which can be performed on a {@link Task}.
 */
public interface TaskActions {

	/**
	 * Fail the owning task with the given throwable.
	 *
	 * @param cause of the failure
	 */
	void failExternally(Throwable cause);
}
