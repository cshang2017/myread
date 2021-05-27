package org.apache.flink.runtime.operators.coordination;

import java.io.Serializable;

/**
 * Root interface for all requests from the client to a {@link OperatorCoordinator}
 * which requests for a {@link CoordinationResponse}.
 */
public interface CoordinationRequest extends Serializable {}
