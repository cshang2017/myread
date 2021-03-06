package org.apache.flink.api.common;

import org.apache.flink.annotation.Public;

/**
 * An exception thrown to indicate that the composed program is invalid. Examples of invalid programs are
 * operations where crucial parameters are omitted, or functions where the input type and the type signature
 * do not match.
 */
@Public
public class InvalidProgramException extends RuntimeException {
	
	/**
	 * Creates a new exception with no message.
	 */
	public InvalidProgramException() {
		super();
	}
	
	/**
	 * Creates a new exception with the given message.
	 * 
	 * @param message The exception message.
	 */
	public InvalidProgramException(String message) {
		super(message);
	}

	/**
	 * Creates a new exception with the given message and cause.
	 * 
	 * @param message The exception message.
	 * @param e The exception cause.
	 */
	public InvalidProgramException(String message, Throwable e) {
		super(message, e);
	}
}
