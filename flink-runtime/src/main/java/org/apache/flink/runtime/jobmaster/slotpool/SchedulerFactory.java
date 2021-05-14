

package org.apache.flink.runtime.jobmaster.slotpool;

import javax.annotation.Nonnull;

/**
 * Factory interface for {@link Scheduler}.
 */
public interface SchedulerFactory {

	/**
	 * Creates a new scheduler instance that uses the given {@link SlotPool} to allocate slots.
	 */
	@Nonnull
	Scheduler createScheduler(@Nonnull SlotPool slotPool);
}
