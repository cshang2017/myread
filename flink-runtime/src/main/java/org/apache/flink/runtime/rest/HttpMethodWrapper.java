package org.apache.flink.runtime.rest;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpMethod;

/**
 * This class wraps netty's {@link HttpMethod}s into an enum, allowing us to use them in switches.
 */
public enum HttpMethodWrapper {
	GET(HttpMethod.GET),
	POST(HttpMethod.POST),
	DELETE(HttpMethod.DELETE),
	PATCH(HttpMethod.PATCH);

	private HttpMethod nettyHttpMethod;

	HttpMethodWrapper(HttpMethod nettyHttpMethod) {
		this.nettyHttpMethod = nettyHttpMethod;
	}

	public HttpMethod getNettyHttpMethod() {
		return nettyHttpMethod;
	}
}
