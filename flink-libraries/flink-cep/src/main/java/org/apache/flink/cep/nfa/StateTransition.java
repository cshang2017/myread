package org.apache.flink.cep.nfa;

import org.apache.flink.cep.pattern.conditions.IterativeCondition;

import java.io.Serializable;
import java.util.Objects;

/**
 * Represents a transition from one {@link State} to another.
 *
 * @param <T> type of events that are handled by the {@link IterativeCondition}
 */
public class StateTransition<T> implements Serializable {

	private final StateTransitionAction action;
	private final State<T> sourceState;
	private final State<T> targetState;
	private IterativeCondition<T> condition;

	public StateTransition(
			final State<T> sourceState,
			final StateTransitionAction action,
			final State<T> targetState,
			final IterativeCondition<T> condition) {
		this.action = action;
		this.targetState = targetState;
		this.sourceState = sourceState;
		this.condition = condition;
	}

	public StateTransitionAction getAction() {
		return action;
	}

	public State<T> getTargetState() {
		return targetState;
	}

	public State<T> getSourceState() {
		return sourceState;
	}

	public IterativeCondition<T> getCondition() {
		return condition;
	}

	public void setCondition(IterativeCondition<T> condition) {
		this.condition = condition;
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof StateTransition) {
			@SuppressWarnings("unchecked")
			StateTransition<T> other = (StateTransition<T>) obj;

			return action == other.action &&
				sourceState.getName().equals(other.sourceState.getName()) &&
				targetState.getName().equals(other.targetState.getName());
		} else {
			return false;
		}
	}

	@Override
	public int hashCode() {
		// we have to take the name of targetState because the transition might be reflexive
		return Objects.hash(action, targetState.getName(), sourceState.getName());
	}

	@Override
	public String toString() {
		return new StringBuilder()
				.append("StateTransition(")
				.append(action).append(", ")
				.append("from ").append(sourceState.getName())
				.append(" to ").append(targetState.getName())
				.append(condition != null ? ", with condition)" : ")")
				.toString();
	}

}
