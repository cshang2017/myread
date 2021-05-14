package org.apache.flink.runtime.jobmaster;

import org.apache.flink.runtime.jobmaster.slotpool.SlotPool;
import org.apache.flink.runtime.jobmaster.slotpool.SlotProvider;
import org.apache.flink.util.AbstractID;

/**
 * This ID identifies the request for a logical slot from the Execution to the {@link SlotPool}
 * oe {@link SlotProvider}. The logical slot may be a physical slot or a sub-slot thereof, in
 * the case of slot sharing.
 *
 * <p>This ID serves a different purpose than the
 * {@link org.apache.flink.runtime.clusterframework.types.AllocationID AllocationID}, which identifies
 * the request of a physical slot, issued from the SlotPool via the ResourceManager to the TaskManager.
 */
public final class SlotRequestId extends AbstractID {

	public SlotRequestId(long lowerPart, long upperPart) {
		super(lowerPart, upperPart);
	}

	public SlotRequestId() {}

	@Override
	public String toString() {
		return "SlotRequestId{" + super.toString() + '}';
	}
}
