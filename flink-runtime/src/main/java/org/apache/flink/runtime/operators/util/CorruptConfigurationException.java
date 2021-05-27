package org.apache.flink.runtime.operators.util;

/**
 * Exception indicating that the parsed configuration was corrupt.
 * Corruption typically means missing or invalid values.
 * 
 */
public class CorruptConfigurationException extends RuntimeException
{

	/**
	 * Creates a new exception with the given error message.
	 * 
	 * @param message The exception's message.
	 */
	public CorruptConfigurationException(String message) {
		super(message);
	}

	public CorruptConfigurationException(String message, Throwable cause) {
		super(message, cause);
	}
}
