
package org.apache.flink.runtime.rest;

import java.io.IOException;

/**
 * Base class for all connection related exception thrown by the
 * {@link RestClient}.
 */
public class ConnectionException extends IOException {

	public ConnectionException(String message) {
		super(message);
	}

	public ConnectionException(String message, Throwable cause) {
		super(message, cause);
	}

	public ConnectionException(Throwable cause) {
		super(cause);
	}
}
