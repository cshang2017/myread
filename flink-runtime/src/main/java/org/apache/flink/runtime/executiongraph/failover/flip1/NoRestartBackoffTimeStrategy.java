

package org.apache.flink.runtime.executiongraph.failover.flip1;

/**
 * Restart strategy which does not restart tasks when tasks fail.
 */
public enum  NoRestartBackoffTimeStrategy implements RestartBackoffTimeStrategy {
	INSTANCE;

	@Override
	public boolean canRestart() {
		return false;
	}

	@Override
	public long getBackoffTime() {
		return 0L;
	}

	@Override
	public void notifyFailure(final Throwable cause) {
		// nothing to do
	}

	@Override
	public String toString() {
		return "NoRestartBackoffTimeStrategy";
	}

	/**
	 * The factory for creating {@link NoRestartBackoffTimeStrategy}.
	 */
	public enum NoRestartBackoffTimeStrategyFactory implements Factory {
		INSTANCE;

		@Override
		public RestartBackoffTimeStrategy create() {
			return NoRestartBackoffTimeStrategy.INSTANCE;
		}

		@Override
		public String toString() {
			return "NoRestartBackoffTimeStrategyFactory";
		}
	}
}
