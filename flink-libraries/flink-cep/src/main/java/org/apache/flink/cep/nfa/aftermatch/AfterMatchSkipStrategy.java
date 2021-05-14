package org.apache.flink.cep.nfa.aftermatch;

import org.apache.flink.cep.nfa.ComputationState;
import org.apache.flink.cep.nfa.sharedbuffer.EventId;
import org.apache.flink.cep.nfa.sharedbuffer.SharedBufferAccessor;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;


/**
 * Indicate the skip strategy after a match process.
 */
public abstract class AfterMatchSkipStrategy implements Serializable {

	private static final long serialVersionUID = -4048930333619068531L;

	/**
	 * Discards every partial match that started before the first event of emitted match mapped to *PatternName*.
	 *
	 * @param patternName the pattern name to skip to
	 * @return the created AfterMatchSkipStrategy
	 */
	public static SkipToFirstStrategy skipToFirst(String patternName) {
		return new SkipToFirstStrategy(patternName, false);
	}

	/**
	 * Discards every partial match that started before the last event of emitted match mapped to *PatternName*.
	 *
	 * @param patternName the pattern name to skip to
	 * @return the created AfterMatchSkipStrategy
	 */
	public static SkipToLastStrategy skipToLast(String patternName) {
		return new SkipToLastStrategy(patternName, false);
	}

	/**
	 * Discards every partial match that started before emitted match ended.
	 *
	 * @return the created AfterMatchSkipStrategy
	 */
	public static SkipPastLastStrategy skipPastLastEvent() {
		return SkipPastLastStrategy.INSTANCE;
	}

	/**
	 * Discards every partial match that started with the same event, emitted match was started.
	 *
	 * @return the created AfterMatchSkipStrategy
	 */
	public static AfterMatchSkipStrategy skipToNext() {
		return SkipToNextStrategy.INSTANCE;
	}

	/**
	 * Every possible match will be emitted.
	 *
	 * @return the created AfterMatchSkipStrategy
	 */
	public static NoSkipStrategy noSkip() {
		return NoSkipStrategy.INSTANCE;
	}

	/**
	 * Tells if the strategy may skip some matches.
	 *
	 * @return false if the strategy is NO_SKIP strategy
	 */
	public abstract boolean isSkipStrategy();

	/**
	 * Prunes matches/partial matches based on the chosen strategy.
	 *
	 * @param matchesToPrune current partial matches
	 * @param matchedResult  already completed matches
	 * @param sharedBufferAccessor   accessor to corresponding shared buffer
	 * @throws Exception thrown if could not access the state
	 */
	public void prune(
			Collection<ComputationState> matchesToPrune,
			Collection<Map<String, List<EventId>>> matchedResult,
			SharedBufferAccessor<?> sharedBufferAccessor) throws Exception {

		EventId pruningId = getPruningId(matchedResult);
		if (pruningId != null) {
			List<ComputationState> discardStates = new ArrayList<>();
			for (ComputationState computationState : matchesToPrune) {
				if (computationState.getStartEventID() != null &&
					shouldPrune(computationState.getStartEventID(), pruningId)) {
					sharedBufferAccessor.releaseNode(computationState.getPreviousBufferEntry());
					discardStates.add(computationState);
				}
			}
			matchesToPrune.removeAll(discardStates);
		}
	}

	/**
	 * Tells if the partial/completed match starting at given id should be prunned by given pruningId.
	 *
	 * @param startEventID starting event id of a partial/completed match
	 * @param pruningId   pruningId calculated by this strategy
	 * @return true if the match should be pruned
	 */
	protected abstract boolean shouldPrune(EventId startEventID, EventId pruningId);

	/**
	 * Retrieves event id of the pruning element from the given match based on the strategy.
	 *
	 * @param match match corresponding to which should the pruning happen
	 * @return pruning event id
	 */
	protected abstract EventId getPruningId(Collection<Map<String, List<EventId>>> match);

	/**
	 * Name of pattern that processing will be skipped to.
	 */
	public Optional<String> getPatternName() {
		return Optional.empty();
	}

	static EventId max(EventId o1, EventId o2) {
		if (o2 == null) {
			return o1;
		}

		if (o1 == null) {
			return o2;
		}

		if (o1.compareTo(o2) >= 0) {
			return o1;
		} else {
			return o2;
		}
	}

	static EventId min(EventId o1, EventId o2) {
		if (o2 == null) {
			return o1;
		}

		if (o1 == null) {
			return o2;
		}

		if (o1.compareTo(o2) <= 0) {
			return o1;
		} else {
			return o2;
		}
	}

	/** Forbid further extending. */
	AfterMatchSkipStrategy() {
	}
}
