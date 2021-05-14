

package org.apache.flink.streaming.api.functions.query;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.util.Preconditions;

/**
 * Internal operator handling queryable state instances (setup and update).
 *
 * @param <S>  State type
 * @param <IN> Input type
 */
@Internal
abstract class AbstractQueryableStateOperator<S extends State, IN>
		extends AbstractStreamOperator<IN>
		implements OneInputStreamOperator<IN, IN> {


	/** State descriptor for the queryable state instance. */
	protected final StateDescriptor<? extends S, ?> stateDescriptor;

	/**
	 * Name under which the queryable state is registered.
	 */
	protected final String registrationName;

	/**
	 * The state instance created on open. This is updated by the subclasses
	 * of this class, because the state update interface depends on the state
	 * type (e.g. AppendingState#add(IN) vs. ValueState#update(OUT)).
	 */
	protected transient S state;

	public AbstractQueryableStateOperator(
			String registrationName,
			StateDescriptor<? extends S, ?> stateDescriptor) {

		this.registrationName = Preconditions.checkNotNull(registrationName, "Registration name");
		this.stateDescriptor = Preconditions.checkNotNull(stateDescriptor, "State descriptor");

		if (stateDescriptor.isQueryable()) {
			String name = stateDescriptor.getQueryableStateName();
			if (!name.equals(registrationName)) {
				throw new IllegalArgumentException("StateDescriptor already marked as " +
						"queryable with name '" + name + "', but created operator with name '" +
						registrationName + "'.");
			} // else: all good, already registered with same name
		} else {
			stateDescriptor.setQueryable(registrationName);
		}
	}

	@Override
	public void open() throws Exception {
		super.open();
		state = getPartitionedState(stateDescriptor);
	}
}
