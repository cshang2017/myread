package org.apache.flink.runtime.operators.coordination;

import java.io.Serializable;

/**
 * Root interface for all events sent between {@link OperatorCoordinator} and an {@link OperatorEventHandler}.
 */
public interface OperatorEvent extends Serializable {}
