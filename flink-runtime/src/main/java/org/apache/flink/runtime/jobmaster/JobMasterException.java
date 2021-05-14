package org.apache.flink.runtime.jobmaster;

import org.apache.flink.util.FlinkException;

/**
 * Base class for {@link JobMaster} related exceptions.
 */
public class JobMasterException extends FlinkException {


	public JobMasterException(String message) {
		super(message);
	}

	public JobMasterException(Throwable cause) {
		super(cause);
	}

	public JobMasterException(String message, Throwable cause) {
		super(message, cause);
	}
}
