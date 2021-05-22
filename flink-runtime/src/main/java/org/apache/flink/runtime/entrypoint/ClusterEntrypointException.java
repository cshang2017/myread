package org.apache.flink.runtime.entrypoint;

import org.apache.flink.util.FlinkException;

/**
 * Exceptions thrown by the {@link ClusterEntrypoint}.
 */
public class ClusterEntrypointException extends FlinkException {

	public ClusterEntrypointException(String message) {
		super(message);
	}

	public ClusterEntrypointException(Throwable cause) {
		super(cause);
	}

	public ClusterEntrypointException(String message, Throwable cause) {
		super(message, cause);
	}
}
