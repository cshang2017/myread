package org.apache.flink.runtime.jobmaster.slotpool;

import org.apache.flink.api.common.JobID;

import javax.annotation.Nonnull;

/**
 * Factory interface for {@link SlotPool}.
 */
public interface SlotPoolFactory {

	@Nonnull
	SlotPool createSlotPool(@Nonnull JobID jobId);
}
