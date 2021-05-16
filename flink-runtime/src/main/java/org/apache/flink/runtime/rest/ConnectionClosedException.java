package org.apache.flink.runtime.rest;

/**
 * Exception which is thrown if the {@link RestClient} detects that a connection
 * was closed.
 */
public class ConnectionClosedException extends ConnectionException {

	public ConnectionClosedException(String message) {
		super(message);
	}

	public ConnectionClosedException(String message, Throwable cause) {
		super(message, cause);
	}

	public ConnectionClosedException(Throwable cause) {
		super(cause);
	}
}
