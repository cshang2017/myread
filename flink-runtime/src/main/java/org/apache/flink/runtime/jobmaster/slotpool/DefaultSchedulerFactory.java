

package org.apache.flink.runtime.jobmaster.slotpool;

import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.ClusterOptions;
import org.apache.flink.configuration.Configuration;

import javax.annotation.Nonnull;

/**
 * Default implementation of a {@link SchedulerFactory}.
 */
public class DefaultSchedulerFactory implements SchedulerFactory {

	@Nonnull
	private final SlotSelectionStrategy slotSelectionStrategy;

	public DefaultSchedulerFactory(@Nonnull SlotSelectionStrategy slotSelectionStrategy) {
		this.slotSelectionStrategy = slotSelectionStrategy;
	}

	@Nonnull
	@Override
	public Scheduler createScheduler(@Nonnull SlotPool slotPool) {
		return new SchedulerImpl(slotSelectionStrategy, slotPool);
	}

	@Nonnull
	private static SlotSelectionStrategy selectSlotSelectionStrategy(@Nonnull Configuration configuration) {
		final boolean evenlySpreadOutSlots = configuration.getBoolean(ClusterOptions.EVENLY_SPREAD_OUT_SLOTS_STRATEGY);

		final SlotSelectionStrategy locationPreferenceSlotSelectionStrategy;

		if (evenlySpreadOutSlots) {
			locationPreferenceSlotSelectionStrategy = LocationPreferenceSlotSelectionStrategy.createEvenlySpreadOut();
		} else {
			locationPreferenceSlotSelectionStrategy = LocationPreferenceSlotSelectionStrategy.createDefault();
		}

		if (configuration.getBoolean(CheckpointingOptions.LOCAL_RECOVERY)) {
			return PreviousAllocationSlotSelectionStrategy.create(locationPreferenceSlotSelectionStrategy);
		} else {
			return locationPreferenceSlotSelectionStrategy;
		}
	}

	public static DefaultSchedulerFactory fromConfiguration(
		@Nonnull Configuration configuration) {
		return new DefaultSchedulerFactory(selectSlotSelectionStrategy(configuration));
	}
}
