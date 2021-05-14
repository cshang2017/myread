package org.apache.flink.cep.nfa;

/**
 * Set of actions when doing a state transition from a {@link State} to another.
 */
public enum StateTransitionAction {
	TAKE, // take the current event and assign it to the current state
	IGNORE, // ignore the current event
	PROCEED // do the state transition and keep the current event for further processing (epsilon transition)
}
