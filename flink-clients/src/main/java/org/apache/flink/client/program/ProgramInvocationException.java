package org.apache.flink.client.program;

import org.apache.flink.api.common.JobID;

/**
 * Exception used to indicate that there is an error during the invocation of a Flink program.
 */
public class ProgramInvocationException extends Exception {
	/**
	 * Serial version UID for serialization interoperability.
	 */

	/**
	 * Creates a <tt>ProgramInvocationException</tt> with the given message.
	 *
	 * @param message
	 *        The message for the exception.
	 */
	public ProgramInvocationException(String message) {
		super(message);
	}

	/**
	 * Creates a <tt>ProgramInvocationException</tt> with the given message which contains job id.
	 *
	 * @param message
	 *        The additional message.
	 * @param jobID
	 *        ID of failed job.
	 */
	public ProgramInvocationException(String message, JobID jobID) {
		super(message + " (JobID: " + jobID + ")");
	}

	/**
	 * Creates a <tt>ProgramInvocationException</tt> for the given exception.
	 *
	 * @param cause
	 *        The exception that causes the program invocation to fail.
	 */
	public ProgramInvocationException(Throwable cause) {
		super(cause);
	}

	/**
	 * Creates a <tt>ProgramInvocationException</tt> for the given exception with an
	 * additional message.
	 *
	 * @param message
	 *        The additional message.
	 * @param cause
	 *        The exception that causes the program invocation to fail.
	 */
	public ProgramInvocationException(String message, Throwable cause) {
		super(message, cause);
	}

	/**
	 * Creates a <tt>ProgramInvocationException</tt> for the given exception with an
	 * additional message which contains job id.
	 *
	 * @param message
	 *        The additional message.
	 * @param jobID
	 *        ID of failed job.
	 * @param cause
	 *        The exception that causes the program invocation to fail.
	 */
	public ProgramInvocationException(String message, JobID jobID, Throwable cause) {
		super(message + " (JobID: " + jobID + ")", cause);
	}
}
