
package org.apache.flink.runtime.rest;

import org.apache.flink.runtime.rest.handler.RestHandlerException;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

/**
 * A special exception that indicates that an element was not found and that the
 * request should be answered with a {@code 404} return code.
 */
public class NotFoundException extends RestHandlerException {

	public NotFoundException(String message) {
		super(message, HttpResponseStatus.NOT_FOUND);
	}

	public NotFoundException(String message, Throwable cause) {
		super(message, HttpResponseStatus.NOT_FOUND, cause);
	}
}
