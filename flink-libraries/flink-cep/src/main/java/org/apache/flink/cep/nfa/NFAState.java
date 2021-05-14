package org.apache.flink.cep.nfa;

import java.util.Arrays;
import java.util.Comparator;
import java.util.Objects;
import java.util.PriorityQueue;
import java.util.Queue;

/**
 * State kept for a {@link NFA}.
 */
public class NFAState {

	/**
	 * Current set of {@link ComputationState computation states} within the state machine.
	 * These are the "active" intermediate states that are waiting for new matching
	 * events to transition to new valid states.
	 */
	private Queue<ComputationState> partialMatches;

	private Queue<ComputationState> completedMatches;

	/**
	 * Flag indicating whether the matching status of the state machine has changed.
	 */
	private boolean stateChanged;

	public static final Comparator<ComputationState> COMPUTATION_STATE_COMPARATOR =
		Comparator.<ComputationState>comparingLong(c ->
				c.getStartEventID() != null ? c.getStartEventID().getTimestamp() : Long.MAX_VALUE)
			.thenComparingInt(c ->
				c.getStartEventID() != null ? c.getStartEventID().getId() : Integer.MAX_VALUE);

	public NFAState(Iterable<ComputationState> states) {
		this.partialMatches = new PriorityQueue<>(COMPUTATION_STATE_COMPARATOR);
		for (ComputationState startingState : states) {
			partialMatches.add(startingState);
		}

		this.completedMatches = new PriorityQueue<>(COMPUTATION_STATE_COMPARATOR);
	}

	public NFAState(Queue<ComputationState> partialMatches, Queue<ComputationState> completedMatches) {
		this.partialMatches = partialMatches;
		this.completedMatches = completedMatches;
	}

	/**
	 * Check if the matching status of the NFA has changed so far.
	 *
	 * @return {@code true} if matching status has changed, {@code false} otherwise
	 */
	public boolean isStateChanged() {
		return stateChanged;
	}

	/**
	 * Reset the changed bit checked via {@link #isStateChanged()} to {@code false}.
	 */
	public void resetStateChanged() {
		this.stateChanged = false;
	}

	/**
	 * Set the changed bit checked via {@link #isStateChanged()} to {@code true}.
	 */
	public void setStateChanged() {
		this.stateChanged = true;
	}

	public Queue<ComputationState> getPartialMatches() {
		return partialMatches;
	}

	public Queue<ComputationState> getCompletedMatches() {
		return completedMatches;
	}

	public void setNewPartialMatches(PriorityQueue<ComputationState> newPartialMatches) {
		this.partialMatches = newPartialMatches;
	}

}
