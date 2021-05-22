package org.apache.flink.runtime.dispatcher;

import org.apache.flink.util.FlinkException;

/**
 * Base class for {@link Dispatcher} related exceptions.
 */
public class DispatcherException extends FlinkException {

	public DispatcherException(String message) {
		super(message);
	}

	public DispatcherException(Throwable cause) {
		super(cause);
	}

	public DispatcherException(String message, Throwable cause) {
		super(message, cause);
	}
}
