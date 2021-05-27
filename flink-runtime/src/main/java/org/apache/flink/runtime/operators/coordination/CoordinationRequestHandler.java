package org.apache.flink.runtime.operators.coordination;

import java.util.concurrent.CompletableFuture;

/**
 * Coordinator interface which can handle {@link CoordinationRequest}s
 * and response with {@link CoordinationResponse}s to the client.
 */
public interface CoordinationRequestHandler {

	/**
	 * Called when receiving a request from the client.
	 *
	 * @param request the request received
	 * @return a future containing the response from the coordinator for this request
	 */
	CompletableFuture<CoordinationResponse> handleCoordinationRequest(CoordinationRequest request);
}
