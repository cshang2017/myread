package org.apache.flink.runtime.minicluster;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.runtime.jobgraph.JobGraph;

/**
 * Interface for {@link JobGraph} executors.
 */
public interface JobExecutor {

	/**
	 * Run the given job and block until its execution result can be returned.
	 *
	 * @param jobGraph to execute
	 * @return Execution result of the executed job
	 */
	JobExecutionResult executeJobBlocking(final JobGraph jobGraph) ;
}
