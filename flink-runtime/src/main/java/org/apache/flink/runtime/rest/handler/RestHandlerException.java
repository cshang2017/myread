package org.apache.flink.runtime.rest.handler;

import org.apache.flink.util.FlinkException;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

/**
 * An exception that is thrown if the failure of a REST operation was detected by a handler.
 */
public class RestHandlerException extends FlinkException {

	private final int responseCode;

	public RestHandlerException(String errorMessage, HttpResponseStatus httpResponseStatus) {
		super(errorMessage);
		this.responseCode = httpResponseStatus.code();
	}

	public RestHandlerException(String errorMessage, HttpResponseStatus httpResponseStatus, Throwable cause) {
		super(errorMessage, cause);
		this.responseCode = httpResponseStatus.code();
	}

	public HttpResponseStatus getHttpResponseStatus() {
		return HttpResponseStatus.valueOf(responseCode);
	}
}
