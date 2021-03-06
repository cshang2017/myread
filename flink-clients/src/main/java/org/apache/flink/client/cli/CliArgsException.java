package org.apache.flink.client.cli;

/**
 * Special exception that is thrown when the command line parsing fails.
 */
public class CliArgsException extends Exception {

	private static final long serialVersionUID = 1L;

	public CliArgsException(String message) {
		super(message);
	}

	public CliArgsException(String message, Throwable cause) {
		super(message, cause);
	}
}
