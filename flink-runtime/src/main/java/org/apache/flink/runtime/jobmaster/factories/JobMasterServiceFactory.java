package org.apache.flink.runtime.jobmaster.factories;

import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobmanager.OnCompletionActions;
import org.apache.flink.runtime.jobmaster.JobMasterService;

/**
 * Factory for a {@link JobMasterService}.
 */
public interface JobMasterServiceFactory {

	JobMasterService createJobMasterService(
		JobGraph jobGraph,
		OnCompletionActions jobCompletionActions,
		ClassLoader userCodeClassloader) throws Exception;
}
