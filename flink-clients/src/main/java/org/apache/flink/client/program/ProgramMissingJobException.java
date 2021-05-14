package org.apache.flink.client.program;

import org.apache.flink.util.FlinkException;

/**
 * Exception used to indicate that no job was executed during the invocation of a Flink program.
 */
public class ProgramMissingJobException extends FlinkException {
	/**
	 * Serial version UID for serialization interoperability.
	 */
	private static final long serialVersionUID = -1964276369605091101L;

	public ProgramMissingJobException(String message) {
		super(message);
	}
}
