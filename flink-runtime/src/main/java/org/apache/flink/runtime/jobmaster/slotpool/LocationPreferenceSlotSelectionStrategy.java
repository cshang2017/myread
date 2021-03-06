package org.apache.flink.runtime.jobmaster.slotpool;

import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.clusterframework.types.SlotProfile;
import org.apache.flink.runtime.jobmanager.scheduler.Locality;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;

import javax.annotation.Nonnull;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * This class implements a {@link SlotSelectionStrategy} that is based on location preference hints.
 */
public abstract class LocationPreferenceSlotSelectionStrategy implements SlotSelectionStrategy {

	LocationPreferenceSlotSelectionStrategy() {}

	@Override
	public Optional<SlotInfoAndLocality> selectBestSlotForProfile(
		@Nonnull Collection<SlotInfoAndResources> availableSlots,
		@Nonnull SlotProfile slotProfile) {

		Collection<TaskManagerLocation> locationPreferences = slotProfile.getPreferredLocations();

		if (availableSlots.isEmpty()) {
			return Optional.empty();
		}

		final ResourceProfile resourceProfile = slotProfile.getPhysicalSlotResourceProfile();

		// if we have no location preferences, we can only filter by the additional requirements.
		return locationPreferences.isEmpty() ?
			selectWithoutLocationPreference(availableSlots, resourceProfile) :
			selectWitLocationPreference(availableSlots, locationPreferences, resourceProfile);
	}

	@Nonnull
	private Optional<SlotInfoAndLocality> selectWitLocationPreference(
		@Nonnull Collection<SlotInfoAndResources> availableSlots,
		@Nonnull Collection<TaskManagerLocation> locationPreferences,
		@Nonnull ResourceProfile resourceProfile) {

		// we build up two indexes, one for resource id and one for host names of the preferred locations.
		final Map<ResourceID, Integer> preferredResourceIDs = new HashMap<>(locationPreferences.size());
		final Map<String, Integer> preferredFQHostNames = new HashMap<>(locationPreferences.size());

		for (TaskManagerLocation locationPreference : locationPreferences) {
			preferredResourceIDs.merge(locationPreference.getResourceID(), 1, Integer::sum);
			preferredFQHostNames.merge(locationPreference.getFQDNHostname(), 1, Integer::sum);
		}

		SlotInfoAndResources bestCandidate = null;
		Locality bestCandidateLocality = Locality.UNKNOWN;
		double bestCandidateScore = Double.NEGATIVE_INFINITY;

		for (SlotInfoAndResources candidate : availableSlots) {

			if (candidate.getRemainingResources().isMatching(resourceProfile)) {

				// this gets candidate is local-weigh
				int localWeigh = preferredResourceIDs.getOrDefault(
					candidate.getSlotInfo().getTaskManagerLocation().getResourceID(), 0);

				// this gets candidate is host-local-weigh
				int hostLocalWeigh = preferredFQHostNames.getOrDefault(
					candidate.getSlotInfo().getTaskManagerLocation().getFQDNHostname(), 0);

				double candidateScore = calculateCandidateScore(localWeigh, hostLocalWeigh, candidate.getTaskExecutorUtilization());
				if (candidateScore > bestCandidateScore) {
					bestCandidateScore = candidateScore;
					bestCandidate = candidate;
					bestCandidateLocality = localWeigh > 0 ?
						Locality.LOCAL : hostLocalWeigh > 0 ?
						Locality.HOST_LOCAL : Locality.NON_LOCAL;
				}
			}
		}

		// at the end of the iteration, we return the candidate with best possible locality or null.
		return bestCandidate != null ?
			Optional.of(SlotInfoAndLocality.of(bestCandidate.getSlotInfo(), bestCandidateLocality)) :
			Optional.empty();
	}

	@Nonnull
	protected abstract Optional<SlotInfoAndLocality> selectWithoutLocationPreference(
		@Nonnull Collection<SlotInfoAndResources> availableSlots,
		@Nonnull ResourceProfile resourceProfile);

	protected abstract double calculateCandidateScore(int localWeigh, int hostLocalWeigh, double taskExecutorUtilization);

	// -------------------------------------------------------------------------------------------
	// Factory methods
	// -------------------------------------------------------------------------------------------

	public static LocationPreferenceSlotSelectionStrategy createDefault() {
		return new DefaultLocationPreferenceSlotSelectionStrategy();
	}

	public static LocationPreferenceSlotSelectionStrategy createEvenlySpreadOut() {
		return new EvenlySpreadOutLocationPreferenceSlotSelectionStrategy();
	}
}
