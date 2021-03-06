package org.apache.flink.runtime.jobgraph.tasks;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.checkpoint.CheckpointCoordinator;
import org.apache.flink.runtime.checkpoint.CheckpointRetentionPolicy;
import org.apache.flink.util.Preconditions;

import java.io.Serializable;
import java.util.Objects;

/**
 * Configuration settings for the {@link CheckpointCoordinator}. This includes the checkpoint
 * interval, the checkpoint timeout, the pause between checkpoints, the maximum number of
 * concurrent checkpoints and settings for externalized checkpoints.
 */
public class CheckpointCoordinatorConfiguration implements Serializable {

	public static final long MINIMAL_CHECKPOINT_TIME = 10;

	private final long checkpointInterval;

	private final long checkpointTimeout;

	private final long minPauseBetweenCheckpoints;

	private final int maxConcurrentCheckpoints;

	private final int tolerableCheckpointFailureNumber;

	/** Settings for what to do with checkpoints when a job finishes. */
	private final CheckpointRetentionPolicy checkpointRetentionPolicy;

	/**
	 * Flag indicating whether exactly once checkpoint mode has been configured.
	 * If <code>false</code>, at least once mode has been configured. This is
	 * not a necessary attribute, because the checkpointing mode is only relevant
	 * for the stream tasks, but we expose it here to forward it to the web runtime
	 * UI.
	 */
	private final boolean isExactlyOnce;

	private final boolean isPreferCheckpointForRecovery;

	private final boolean isUnalignedCheckpointsEnabled;

	/**
	 * @deprecated use {@link #builder()}.
	 */
	@Deprecated
	@VisibleForTesting
	public CheckpointCoordinatorConfiguration(
			long checkpointInterval,
			long checkpointTimeout,
			long minPauseBetweenCheckpoints,
			int maxConcurrentCheckpoints,
			CheckpointRetentionPolicy checkpointRetentionPolicy,
			boolean isExactlyOnce,
			boolean isUnalignedCheckpoint,
			boolean isPreferCheckpointForRecovery,
			int tolerableCpFailureNumber) {
		this(
			checkpointInterval,
			checkpointTimeout,
			minPauseBetweenCheckpoints,
			maxConcurrentCheckpoints,
			checkpointRetentionPolicy,
			isExactlyOnce,
			isPreferCheckpointForRecovery,
			tolerableCpFailureNumber,
			isUnalignedCheckpoint);
	}

	private CheckpointCoordinatorConfiguration(
			long checkpointInterval,
			long checkpointTimeout,
			long minPauseBetweenCheckpoints,
			int maxConcurrentCheckpoints,
			CheckpointRetentionPolicy checkpointRetentionPolicy,
			boolean isExactlyOnce,
			boolean isPreferCheckpointForRecovery,
			int tolerableCpFailureNumber,
			boolean isUnalignedCheckpointsEnabled) {

		// sanity checks
		if (checkpointInterval < MINIMAL_CHECKPOINT_TIME || checkpointTimeout < MINIMAL_CHECKPOINT_TIME ||
			minPauseBetweenCheckpoints < 0 || maxConcurrentCheckpoints < 1 ||
			tolerableCpFailureNumber < 0) {
			throw new IllegalArgumentException();
		}
		Preconditions.checkArgument(!isUnalignedCheckpointsEnabled || maxConcurrentCheckpoints <= 1,
				"maxConcurrentCheckpoints can't be > 1 if UnalignedCheckpoints enabled");

		this.checkpointInterval = checkpointInterval;
		this.checkpointTimeout = checkpointTimeout;
		this.minPauseBetweenCheckpoints = minPauseBetweenCheckpoints;
		this.maxConcurrentCheckpoints = maxConcurrentCheckpoints;
		this.checkpointRetentionPolicy = Preconditions.checkNotNull(checkpointRetentionPolicy);
		this.isExactlyOnce = isExactlyOnce;
		this.isPreferCheckpointForRecovery = isPreferCheckpointForRecovery;
		this.tolerableCheckpointFailureNumber = tolerableCpFailureNumber;
		this.isUnalignedCheckpointsEnabled = isUnalignedCheckpointsEnabled;
	}

	public long getCheckpointInterval() {
		return checkpointInterval;
	}

	public long getCheckpointTimeout() {
		return checkpointTimeout;
	}

	public long getMinPauseBetweenCheckpoints() {
		return minPauseBetweenCheckpoints;
	}

	public int getMaxConcurrentCheckpoints() {
		return maxConcurrentCheckpoints;
	}

	public CheckpointRetentionPolicy getCheckpointRetentionPolicy() {
		return checkpointRetentionPolicy;
	}

	public boolean isExactlyOnce() {
		return isExactlyOnce;
	}

	public boolean isPreferCheckpointForRecovery() {
		return isPreferCheckpointForRecovery;
	}

	public int getTolerableCheckpointFailureNumber() {
		return tolerableCheckpointFailureNumber;
	}

	public boolean isUnalignedCheckpointsEnabled() {
		return isUnalignedCheckpointsEnabled;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		CheckpointCoordinatorConfiguration that = (CheckpointCoordinatorConfiguration) o;
		return checkpointInterval == that.checkpointInterval &&
			checkpointTimeout == that.checkpointTimeout &&
			minPauseBetweenCheckpoints == that.minPauseBetweenCheckpoints &&
			maxConcurrentCheckpoints == that.maxConcurrentCheckpoints &&
			isExactlyOnce == that.isExactlyOnce &&
			isUnalignedCheckpointsEnabled == that.isUnalignedCheckpointsEnabled &&
			checkpointRetentionPolicy == that.checkpointRetentionPolicy &&
			isPreferCheckpointForRecovery == that.isPreferCheckpointForRecovery &&
			tolerableCheckpointFailureNumber == that.tolerableCheckpointFailureNumber;
	}

	@Override
	public int hashCode() {
		return Objects.hash(
				checkpointInterval,
				checkpointTimeout,
				minPauseBetweenCheckpoints,
				maxConcurrentCheckpoints,
				checkpointRetentionPolicy,
				isExactlyOnce,
				isUnalignedCheckpointsEnabled,
				isPreferCheckpointForRecovery,
				tolerableCheckpointFailureNumber);
	}

	@Override
	public String toString() {
		return "JobCheckpointingConfiguration{" +
			"checkpointInterval=" + checkpointInterval +
			", checkpointTimeout=" + checkpointTimeout +
			", minPauseBetweenCheckpoints=" + minPauseBetweenCheckpoints +
			", maxConcurrentCheckpoints=" + maxConcurrentCheckpoints +
			", checkpointRetentionPolicy=" + checkpointRetentionPolicy +
			", isExactlyOnce=" + isExactlyOnce +
			", isUnalignedCheckpoint=" + isUnalignedCheckpointsEnabled +
			", isPreferCheckpointForRecovery=" + isPreferCheckpointForRecovery +
			", tolerableCheckpointFailureNumber=" + tolerableCheckpointFailureNumber +
			'}';
	}

	public static CheckpointCoordinatorConfigurationBuilder builder() {
		return new CheckpointCoordinatorConfigurationBuilder();
	}

	/**
	 * {@link CheckpointCoordinatorConfiguration} builder.
	 */
	public static class CheckpointCoordinatorConfigurationBuilder {
		private long checkpointInterval = MINIMAL_CHECKPOINT_TIME;
		private long checkpointTimeout = MINIMAL_CHECKPOINT_TIME;
		private long minPauseBetweenCheckpoints;
		private int maxConcurrentCheckpoints = 1;
		private CheckpointRetentionPolicy checkpointRetentionPolicy = CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION;
		private boolean isExactlyOnce = true;
		private boolean isPreferCheckpointForRecovery = true;
		private int tolerableCheckpointFailureNumber;
		private boolean isUnalignedCheckpointsEnabled;

		public CheckpointCoordinatorConfiguration build() {
			return new CheckpointCoordinatorConfiguration(
				checkpointInterval,
				checkpointTimeout,
				minPauseBetweenCheckpoints,
				maxConcurrentCheckpoints,
				checkpointRetentionPolicy,
				isExactlyOnce,
				isPreferCheckpointForRecovery,
				tolerableCheckpointFailureNumber,
				isUnalignedCheckpointsEnabled
			);
		}

		public CheckpointCoordinatorConfigurationBuilder setCheckpointInterval(long checkpointInterval) {
			this.checkpointInterval = checkpointInterval;
			return this;
		}

		public CheckpointCoordinatorConfigurationBuilder setCheckpointTimeout(long checkpointTimeout) {
			this.checkpointTimeout = checkpointTimeout;
			return this;
		}

		public CheckpointCoordinatorConfigurationBuilder setMinPauseBetweenCheckpoints(long minPauseBetweenCheckpoints) {
			this.minPauseBetweenCheckpoints = minPauseBetweenCheckpoints;
			return this;
		}

		public CheckpointCoordinatorConfigurationBuilder setMaxConcurrentCheckpoints(int maxConcurrentCheckpoints) {
			this.maxConcurrentCheckpoints = maxConcurrentCheckpoints;
			return this;
		}

		public CheckpointCoordinatorConfigurationBuilder setCheckpointRetentionPolicy(CheckpointRetentionPolicy checkpointRetentionPolicy) {
			this.checkpointRetentionPolicy = checkpointRetentionPolicy;
			return this;
		}

		public CheckpointCoordinatorConfigurationBuilder setExactlyOnce(boolean exactlyOnce) {
			isExactlyOnce = exactlyOnce;
			return this;
		}

		public CheckpointCoordinatorConfigurationBuilder setPreferCheckpointForRecovery(boolean preferCheckpointForRecovery) {
			isPreferCheckpointForRecovery = preferCheckpointForRecovery;
			return this;
		}

		public CheckpointCoordinatorConfigurationBuilder setTolerableCheckpointFailureNumber(int tolerableCheckpointFailureNumber) {
			this.tolerableCheckpointFailureNumber = tolerableCheckpointFailureNumber;
			return this;
		}

		public CheckpointCoordinatorConfigurationBuilder setUnalignedCheckpointsEnabled(boolean unalignedCheckpointsEnabled) {
			isUnalignedCheckpointsEnabled = unalignedCheckpointsEnabled;
			return this;
		}
	}
}
