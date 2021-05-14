package org.apache.flink.cep.nfa;

import org.apache.flink.cep.pattern.conditions.IterativeCondition;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Objects;

/**
 * Represents a state of the {@link NFA}.
 *
 * <p>Each state is identified by a name and a state type. Furthermore, it contains a collection of
 * state transitions. The state transitions describe under which conditions it is possible to enter
 * a new state.
 *
 * @param <T> Type of the input events
 */
public class State<T> implements Serializable {

	private final String name;
	private StateType stateType;
	private final Collection<StateTransition<T>> stateTransitions;

	public State(final String name, final StateType stateType) {
		this.name = name;
		this.stateType = stateType;

		stateTransitions = new ArrayList<>();
	}

	public StateType getStateType() {
		return stateType;
	}

	public boolean isFinal() {
		return stateType == StateType.Final;
	}

	public boolean isStart() {
		return stateType == StateType.Start;
	}

	public String getName() {
		return name;
	}

	public Collection<StateTransition<T>> getStateTransitions() {
		return stateTransitions;
	}

	public void makeStart() {
		this.stateType = StateType.Start;
	}

	public void addStateTransition(
			final StateTransitionAction action,
			final State<T> targetState,
			final IterativeCondition<T> condition) {
		stateTransitions.add(new StateTransition<T>(this, action, targetState, condition));
	}

	public void addIgnore(final IterativeCondition<T> condition) {
		addStateTransition(StateTransitionAction.IGNORE, this, condition);
	}

	public void addIgnore(final State<T> targetState, final IterativeCondition<T> condition) {
		addStateTransition(StateTransitionAction.IGNORE, targetState, condition);
	}

	public void addTake(final State<T> targetState, final IterativeCondition<T> condition) {
		addStateTransition(StateTransitionAction.TAKE, targetState, condition);
	}

	public void addProceed(final State<T> targetState, final IterativeCondition<T> condition) {
		addStateTransition(StateTransitionAction.PROCEED, targetState, condition);
	}

	public void addTake(final IterativeCondition<T> condition) {
		addStateTransition(StateTransitionAction.TAKE, this, condition);
	}


	public boolean isStop() {
		return stateType == StateType.Stop;
	}

	/**
	 * Set of valid state types.
	 */
	public enum StateType {
		Start, // the state is a starting state for the NFA
		Final, // the state is a final state for the NFA
		Normal, // the state is neither a start nor a final state
		Stop
	}
}
