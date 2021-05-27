

package org.apache.flink.runtime.operators.coordination;

import org.apache.flink.runtime.jobgraph.OperatorID;

/**
 * The dispatcher through which Operators receive {@link OperatorEvent}s and through which they can
 * send OperatorEvents back to the {@code OperatorCoordinator}.
 */
public interface OperatorEventDispatcher {

	/**
	 * Register a listener that is notified every time an OperatorEvent is sent from the
	 * OperatorCoordinator (of the operator with the given OperatorID) to this subtask.
	 */
	void registerEventHandler(OperatorID operator, OperatorEventHandler handler);

	/**
	 * Gets the gateway through which events can be passed to the OperatorCoordinator for
	 * the operator identified by the given OperatorID.
	 */
	OperatorEventGateway getOperatorEventGateway(OperatorID operatorId);
}
