
package org.apache.flink.runtime.jobmaster;

import org.apache.flink.util.FlinkException;

/**
 * Base exception thrown by the {@link JobMaster}.
 */
public class JobManagerException extends FlinkException {


	public JobManagerException(final String message) {
		super(message);
	}

	public JobManagerException(final String message, Throwable cause) {
		super(message, cause);
	}

	public JobManagerException(Throwable cause) {
		super(cause);
	}
}
