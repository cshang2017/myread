package org.apache.flink.runtime.jobmaster.slotpool;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.util.clock.Clock;
import org.apache.flink.util.clock.SystemClock;

import javax.annotation.Nonnull;

/**
 * Default slot pool factory.
 */
public class DefaultSlotPoolFactory implements SlotPoolFactory {

	@Nonnull
	private final Clock clock;

	@Nonnull
	private final Time rpcTimeout;

	@Nonnull
	private final Time slotIdleTimeout;

	@Nonnull
	private final Time batchSlotTimeout;

	public DefaultSlotPoolFactory(
			@Nonnull Clock clock,
			@Nonnull Time rpcTimeout,
			@Nonnull Time slotIdleTimeout,
			@Nonnull Time batchSlotTimeout) {
		this.clock = clock;
		this.rpcTimeout = rpcTimeout;
		this.slotIdleTimeout = slotIdleTimeout;
		this.batchSlotTimeout = batchSlotTimeout;
	}

	@Override
	@Nonnull
	public SlotPool createSlotPool(@Nonnull JobID jobId) {
		return new SlotPoolImpl(
			jobId,
			clock,
			rpcTimeout,
			slotIdleTimeout,
			batchSlotTimeout);
	}

	public static DefaultSlotPoolFactory fromConfiguration(@Nonnull Configuration configuration) {

		final Time rpcTimeout = AkkaUtils.getTimeoutAsTime(configuration);
		final Time slotIdleTimeout = Time.milliseconds(configuration.getLong(JobManagerOptions.SLOT_IDLE_TIMEOUT));
		final Time batchSlotTimeout = Time.milliseconds(configuration.getLong(JobManagerOptions.SLOT_REQUEST_TIMEOUT));

		return new DefaultSlotPoolFactory(
			SystemClock.getInstance(),
			rpcTimeout,
			slotIdleTimeout,
			batchSlotTimeout);
	}
}
