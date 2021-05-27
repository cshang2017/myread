package org.apache.flink.runtime.operators.coordination;

/**
 * Interface for handlers of operator events on the operator side.
 * Operator events are sent between an {@link OperatorCoordinator} and a runtime operator (which registers
 * this handler).
 *
 * <p>The counterpart to this handler is the {@link OperatorCoordinator#handleEventFromOperator(int, OperatorEvent)}
 * method.
 */
public interface OperatorEventHandler {

	void handleOperatorEvent(OperatorEvent evt);
}
