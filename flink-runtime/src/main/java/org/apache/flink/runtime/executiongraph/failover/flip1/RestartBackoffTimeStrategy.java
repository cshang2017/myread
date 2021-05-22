package org.apache.flink.runtime.executiongraph.failover.flip1;

/**
 * Strategy to decide whether to restart failed tasks and the delay to do the restarting.
 */
public interface RestartBackoffTimeStrategy {

	/**
	 * Returns whether a restart should be conducted.
	 *
	 * @return whether a restart should be conducted
	 */
	boolean canRestart();

	/**
	 * Returns the delay to do the restarting.
	 *
	 * @return the delay to do the restarting
	 */
	long getBackoffTime();

	/**
	 * Notify the strategy about the task failure cause.
	 *
	 * @param cause of the task failure
	 */
	void notifyFailure(Throwable cause);

	// ------------------------------------------------------------------------
	//  factory
	// ------------------------------------------------------------------------

	/**
	 * The factory to instantiate {@link RestartBackoffTimeStrategy}.
	 */
	interface Factory {

		/**
		 * Instantiates the {@link RestartBackoffTimeStrategy}.
		 *
		 * @return The instantiated restart strategy.
		 */
		RestartBackoffTimeStrategy create();
	}
}
