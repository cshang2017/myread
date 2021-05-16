package org.apache.flink.runtime.messages;

import org.apache.flink.api.common.JobID;
import org.apache.flink.util.FlinkException;

/**
 * Exception indicating that we could not find a Flink job with the given job ID.
 */
public class FlinkJobNotFoundException extends FlinkException {

	public FlinkJobNotFoundException(JobID jobId) {
		super("Could not find Flink job (" + jobId + ')');
	}
}
