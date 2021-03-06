package org.apache.flink.runtime.jobmaster.slotpool;

import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.clusterframework.types.SlotProfile;
import org.apache.flink.runtime.jobmanager.scheduler.Locality;
import org.apache.flink.runtime.jobmaster.SlotInfo;

import javax.annotation.Nonnull;

import java.util.Collection;
import java.util.Optional;

/**
 * Interface for slot selection strategies to be used in the {@link Scheduler}.
 */
public interface SlotSelectionStrategy {

	/**
	 * Selects the best {@link SlotInfo} w.r.t. a certain selection criterion from the provided list of available slots
	 * and considering the given {@link SlotProfile} that describes the requirements.
	 *
	 * @param availableSlots a list of the available slots together with their remaining resources to select from.
	 * @param slotProfile a slot profile, describing requirements for the slot selection.
	 * @return the selected slot info with the corresponding locality hint.
	 */
	Optional<SlotInfoAndLocality> selectBestSlotForProfile(
		@Nonnull Collection<SlotInfoAndResources> availableSlots,
		@Nonnull SlotProfile slotProfile);

	/**
	 * This class is a value type that combines a {@link SlotInfo} with its remaining {@link ResourceProfile}.
	 */
	final class SlotInfoAndResources {

		@Nonnull
		private final SlotInfo slotInfo;

		@Nonnull
		private final ResourceProfile remainingResources;

		private final double taskExecutorUtilization;

		public SlotInfoAndResources(@Nonnull SlotInfo slotInfo, @Nonnull ResourceProfile remainingResources, double taskExecutorUtilization) {
			this.slotInfo = slotInfo;
			this.remainingResources = remainingResources;
			this.taskExecutorUtilization = taskExecutorUtilization;
		}

		@Nonnull
		public SlotInfo getSlotInfo() {
			return slotInfo;
		}

		@Nonnull
		public ResourceProfile getRemainingResources() {
			return remainingResources;
		}

		public double getTaskExecutorUtilization() {
			return taskExecutorUtilization;
		}

		public static SlotInfoAndResources fromSingleSlot(@Nonnull SlotInfoWithUtilization slotInfoWithUtilization) {
			return new SlotInfoAndResources(
				slotInfoWithUtilization,
				slotInfoWithUtilization.getResourceProfile(),
				slotInfoWithUtilization.getTaskExecutorUtilization());
		}
	}

	/**
	 * This class is a value type that combines a {@link SlotInfo} with a {@link Locality} hint.
	 */
	final class SlotInfoAndLocality {

		@Nonnull
		private final SlotInfo slotInfo;

		@Nonnull
		private final Locality locality;

		private SlotInfoAndLocality(@Nonnull SlotInfo slotInfo, @Nonnull Locality locality) {
			this.slotInfo = slotInfo;
			this.locality = locality;
		}

		@Nonnull
		public SlotInfo getSlotInfo() {
			return slotInfo;
		}

		@Nonnull
		public Locality getLocality() {
			return locality;
		}

		public static SlotInfoAndLocality of(@Nonnull SlotInfo slotInfo, @Nonnull Locality locality) {
			return new SlotInfoAndLocality(slotInfo, locality);
		}
	}
}
