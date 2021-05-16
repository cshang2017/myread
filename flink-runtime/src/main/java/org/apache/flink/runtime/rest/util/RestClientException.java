
package org.apache.flink.runtime.rest.util;

import org.apache.flink.util.FlinkException;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

/**
 * An exception that is thrown if the failure of a REST operation was detected on the client.
 */
public class RestClientException extends FlinkException {

	private final int responseCode;

	public RestClientException(String message, HttpResponseStatus responseStatus) {
		super(message);

		Preconditions.checkNotNull(responseStatus);
		responseCode = responseStatus.code();
	}

	public RestClientException(String message, Throwable cause, HttpResponseStatus responseStatus) {
		super(message, cause);

		responseCode = responseStatus.code();
	}

	public HttpResponseStatus getHttpResponseStatus() {
		return HttpResponseStatus.valueOf(responseCode);
	}
}
