

package org.apache.flink.client.program;

/**
 * A special exception used to abort programs when the caller is only interested in the
 * program plan, rather than in the full execution.
 */
public class ProgramAbortException extends Error {
	private static final long serialVersionUID = 1L;
}
