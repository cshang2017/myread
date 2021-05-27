package org.apache.flink.runtime.operators.coordination;

import org.apache.flink.runtime.jobgraph.OperatorID;

import java.util.concurrent.CompletableFuture;

/**
 * Client interface which sends out a {@link CoordinationRequest} and
 * expects for a {@link CoordinationResponse} from a {@link OperatorCoordinator}.
 */
public interface CoordinationRequestGateway {

	/**
	 * Send out a request to a specified coordinator and return the response.
	 *
	 * @param operatorId specifies which coordinator to receive the request
	 * @param request the request to send
	 * @return the response from the coordinator
	 */
	CompletableFuture<CoordinationResponse> sendCoordinationRequest(OperatorID operatorId, CoordinationRequest request);
}
