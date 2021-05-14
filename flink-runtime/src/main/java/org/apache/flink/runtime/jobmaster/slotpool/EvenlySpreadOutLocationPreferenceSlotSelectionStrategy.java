package org.apache.flink.runtime.jobmaster.slotpool;

import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.jobmanager.scheduler.Locality;

import javax.annotation.Nonnull;

import java.util.Collection;
import java.util.Comparator;
import java.util.Optional;

class EvenlySpreadOutLocationPreferenceSlotSelectionStrategy extends LocationPreferenceSlotSelectionStrategy {
	@Nonnull
	@Override
	protected Optional<SlotInfoAndLocality> selectWithoutLocationPreference(@Nonnull Collection<SlotInfoAndResources> availableSlots, @Nonnull ResourceProfile resourceProfile) {
		return availableSlots.stream()
			.filter(slotInfoAndResources -> slotInfoAndResources.getRemainingResources().isMatching(resourceProfile))
			.min(Comparator.comparing(SlotInfoAndResources::getTaskExecutorUtilization))
			.map(slotInfoAndResources -> SlotInfoAndLocality.of(slotInfoAndResources.getSlotInfo(), Locality.UNCONSTRAINED));
	}

	@Override
	protected double calculateCandidateScore(int localWeigh, int hostLocalWeigh, double taskExecutorUtilization) {
		// taskExecutorUtilization in [0, 1] --> only affects choice if localWeigh and hostLocalWeigh
		// between two candidates are equal
		return localWeigh * 20 + hostLocalWeigh * 2 - taskExecutorUtilization;
	}
}
