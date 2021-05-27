package org.apache.flink.runtime.operators.coordination;

import java.io.Serializable;

/**
 * Root interface for all responses from a {@link OperatorCoordinator} to the client
 * which is the response for a {@link CoordinationRequest}.
 */
public interface CoordinationResponse extends Serializable {}
