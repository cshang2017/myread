package org.apache.flink.runtime.rest;

/**
 * Exception which is thrown by the {@link RestClient} if a connection
 * becomes idle.
 */
public class ConnectionIdleException extends ConnectionException {

	public ConnectionIdleException(String message) {
		super(message);
	}

	public ConnectionIdleException(String message, Throwable cause) {
		super(message, cause);
	}

	public ConnectionIdleException(Throwable cause) {
		super(cause);
	}
}
