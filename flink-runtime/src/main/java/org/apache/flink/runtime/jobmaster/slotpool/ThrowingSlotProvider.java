
package org.apache.flink.runtime.jobmaster.slotpool;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.clusterframework.types.SlotProfile;
import org.apache.flink.runtime.instance.SlotSharingGroupId;
import org.apache.flink.runtime.jobmanager.scheduler.ScheduledUnit;
import org.apache.flink.runtime.jobmaster.LogicalSlot;
import org.apache.flink.runtime.jobmaster.SlotRequestId;

import javax.annotation.Nullable;

import java.util.concurrent.CompletableFuture;

/**
 * A slot provider that validates that it is not in use by throwing {@link IllegalStateException}
 * on any method call.
 */
public class ThrowingSlotProvider implements SlotProvider {

	@Override
	public CompletableFuture<LogicalSlot> allocateSlot(SlotRequestId slotRequestId, ScheduledUnit scheduledUnit, SlotProfile slotProfile, Time allocationTimeout) {
		throw new IllegalStateException("Unexpected allocateSlot() call");
	}

	@Override
	public void cancelSlotRequest(SlotRequestId slotRequestId, @Nullable SlotSharingGroupId slotSharingGroupId, Throwable cause) {
		throw new IllegalStateException("Unexpected cancelSlotRequest() call");
	}
}
