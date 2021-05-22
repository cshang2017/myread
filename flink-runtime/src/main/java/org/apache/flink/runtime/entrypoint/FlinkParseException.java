

package org.apache.flink.runtime.entrypoint;

import org.apache.flink.util.FlinkException;

/**
 * Exception which indicates that the parsing of command line
 * arguments failed.
 */
public class FlinkParseException extends FlinkException {


	public FlinkParseException(String message) {
		super(message);
	}

	public FlinkParseException(Throwable cause) {
		super(cause);
	}

	public FlinkParseException(String message, Throwable cause) {
		super(message, cause);
	}
}
