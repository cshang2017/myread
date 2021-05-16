package org.apache.flink.runtime.resourcemanager.slotmanager;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.AkkaOptions;
import org.apache.flink.configuration.ClusterOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.ResourceManagerOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.resourcemanager.WorkerResourceSpec;
import org.apache.flink.util.ConfigurationException;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Configuration for the {@link SlotManager}.
 */
public class SlotManagerConfiguration {

	private final Time taskManagerRequestTimeout;
	private final Time slotRequestTimeout;
	private final Time taskManagerTimeout;
	private final boolean waitResultConsumedBeforeRelease;
	private final SlotMatchingStrategy slotMatchingStrategy;
	private final WorkerResourceSpec defaultWorkerResourceSpec;
	private final int numSlotsPerWorker;
	private final int maxSlotNum;

	public SlotManagerConfiguration(
			Time taskManagerRequestTimeout,
			Time slotRequestTimeout,
			Time taskManagerTimeout,
			boolean waitResultConsumedBeforeRelease,
			SlotMatchingStrategy slotMatchingStrategy,
			WorkerResourceSpec defaultWorkerResourceSpec,
			int numSlotsPerWorker,
			int maxSlotNum) {

		this.taskManagerRequestTimeout = Preconditions.checkNotNull(taskManagerRequestTimeout);
		this.slotRequestTimeout = Preconditions.checkNotNull(slotRequestTimeout);
		this.taskManagerTimeout = Preconditions.checkNotNull(taskManagerTimeout);
		this.waitResultConsumedBeforeRelease = waitResultConsumedBeforeRelease;
		this.slotMatchingStrategy = Preconditions.checkNotNull(slotMatchingStrategy);
		this.defaultWorkerResourceSpec = Preconditions.checkNotNull(defaultWorkerResourceSpec);
		Preconditions.checkState(numSlotsPerWorker > 0);
		Preconditions.checkState(maxSlotNum > 0);
		this.numSlotsPerWorker = numSlotsPerWorker;
		this.maxSlotNum = maxSlotNum;
	}

	public Time getTaskManagerRequestTimeout() {
		return taskManagerRequestTimeout;
	}

	public Time getSlotRequestTimeout() {
		return slotRequestTimeout;
	}

	public Time getTaskManagerTimeout() {
		return taskManagerTimeout;
	}

	public boolean isWaitResultConsumedBeforeRelease() {
		return waitResultConsumedBeforeRelease;
	}

	public SlotMatchingStrategy getSlotMatchingStrategy() {
		return slotMatchingStrategy;
	}

	public WorkerResourceSpec getDefaultWorkerResourceSpec() {
		return defaultWorkerResourceSpec;
	}

	public int getNumSlotsPerWorker() {
		return numSlotsPerWorker;
	}

	public int getMaxSlotNum() {
		return maxSlotNum;
	}

	public static SlotManagerConfiguration fromConfiguration(
			Configuration configuration,
			WorkerResourceSpec defaultWorkerResourceSpec) throws ConfigurationException {

		final Time rpcTimeout;
			rpcTimeout = AkkaUtils.getTimeoutAsTime(configuration);
		

		final Time slotRequestTimeout = getSlotRequestTimeout(configuration);
		final Time taskManagerTimeout = Time.milliseconds(
				configuration.getLong(ResourceManagerOptions.TASK_MANAGER_TIMEOUT));

		boolean waitResultConsumedBeforeRelease =
			configuration.getBoolean(ResourceManagerOptions.TASK_MANAGER_RELEASE_WHEN_RESULT_CONSUMED);

		boolean evenlySpreadOutSlots = configuration.getBoolean(ClusterOptions.EVENLY_SPREAD_OUT_SLOTS_STRATEGY);
		final SlotMatchingStrategy slotMatchingStrategy = evenlySpreadOutSlots ?
			LeastUtilizationSlotMatchingStrategy.INSTANCE : AnyMatchingSlotMatchingStrategy.INSTANCE;

		int numSlotsPerWorker = configuration.getInteger(TaskManagerOptions.NUM_TASK_SLOTS);

		int maxSlotNum = configuration.getInteger(ResourceManagerOptions.MAX_SLOT_NUM);

		return new SlotManagerConfiguration(
			rpcTimeout,
			slotRequestTimeout,
			taskManagerTimeout,
			waitResultConsumedBeforeRelease,
			slotMatchingStrategy,
			defaultWorkerResourceSpec,
			numSlotsPerWorker,
			maxSlotNum);
	}

	private static Time getSlotRequestTimeout(final Configuration configuration) {
		final long slotRequestTimeoutMs;
		if (configuration.contains(ResourceManagerOptions.SLOT_REQUEST_TIMEOUT)) {
			slotRequestTimeoutMs = configuration.getLong(ResourceManagerOptions.SLOT_REQUEST_TIMEOUT);
		} else {
			slotRequestTimeoutMs = configuration.getLong(JobManagerOptions.SLOT_REQUEST_TIMEOUT);
		}
		return Time.milliseconds(slotRequestTimeoutMs);
	}
}
