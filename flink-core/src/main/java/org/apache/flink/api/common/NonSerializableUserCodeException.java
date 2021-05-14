package org.apache.flink.api.common;

import org.apache.flink.annotation.Public;

/**
 * An special case of the {@link InvalidProgramException}, indicating that a part of the program
 * that needs to be serializable (for shipping) is not serializable.
 */
@Public
public class NonSerializableUserCodeException extends InvalidProgramException {
	
	/**
	 * Creates a new exception with no message.
	 */
	public NonSerializableUserCodeException() {
		super();
	}
	
	/**
	 * Creates a new exception with the given message.
	 * 
	 * @param message The exception message.
	 */
	public NonSerializableUserCodeException(String message) {
		super(message);
	}
}
