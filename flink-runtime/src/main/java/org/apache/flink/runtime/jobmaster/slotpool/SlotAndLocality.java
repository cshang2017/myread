package org.apache.flink.runtime.jobmaster.slotpool;

import org.apache.flink.runtime.jobmanager.scheduler.Locality;

import javax.annotation.Nonnull;

/**
 * A combination of a {@link AllocatedSlot} and a {@link Locality}.
 */
public class SlotAndLocality {

	@Nonnull
	private final PhysicalSlot slot;

	@Nonnull
	private final Locality locality;

	public SlotAndLocality(@Nonnull PhysicalSlot slot, @Nonnull Locality locality) {
		this.slot = slot;
		this.locality = locality;
	}

	// ------------------------------------------------------------------------

	@Nonnull
	public PhysicalSlot getSlot() {
		return slot;
	}

	@Nonnull
	public Locality getLocality() {
		return locality;
	}

	// ------------------------------------------------------------------------

	@Override
	public String toString() {
		return "Slot: " + slot + " (" + locality + ')';
	}
}
