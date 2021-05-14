
package org.apache.flink.runtime.jobmaster.slotpool;

import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.jobmanager.scheduler.Locality;

import javax.annotation.Nonnull;

import java.util.Collection;
import java.util.Optional;

class DefaultLocationPreferenceSlotSelectionStrategy extends LocationPreferenceSlotSelectionStrategy {

	@Nonnull
	@Override
	protected Optional<SlotInfoAndLocality> selectWithoutLocationPreference(@Nonnull Collection<SlotInfoAndResources> availableSlots, @Nonnull ResourceProfile resourceProfile) {
		for (SlotInfoAndResources candidate : availableSlots) {
			if (candidate.getRemainingResources().isMatching(resourceProfile)) {
				return Optional.of(SlotInfoAndLocality.of(candidate.getSlotInfo(), Locality.UNCONSTRAINED));
			}
		}
		return Optional.empty();
	}

	@Override
	protected double calculateCandidateScore(int localWeigh, int hostLocalWeigh, double taskExecutorUtilization) {
		return localWeigh * 10 + hostLocalWeigh;
	}
}
