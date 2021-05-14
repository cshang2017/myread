package org.apache.flink.runtime.jobmaster.slotpool;

import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.jobmaster.SlotOwner;

import javax.annotation.Nonnull;

/**
 * Basic interface for the current scheduler, which is a {@link SlotProvider} and a {@link SlotOwner}.
 */
public interface Scheduler extends SlotProvider, SlotOwner {

	/**
	 * Start the scheduler by initializing the main thread executor.
	 *
	 * @param mainThreadExecutor the main thread executor of the job master.
	 */
	void start(@Nonnull ComponentMainThreadExecutor mainThreadExecutor);

	/**
	 * Returns true, iff the scheduling strategy of the scheduler requires to know about previous allocations.
	 */
	boolean requiresPreviousExecutionGraphAllocations();
}
