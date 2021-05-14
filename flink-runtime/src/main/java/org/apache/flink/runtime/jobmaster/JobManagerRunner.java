package org.apache.flink.runtime.jobmaster;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;
import org.apache.flink.util.AutoCloseableAsync;

import java.util.concurrent.CompletableFuture;

/**
 * Interface for a runner which executes a {@link JobMaster}.
 */
public interface JobManagerRunner extends AutoCloseableAsync {

	/**
	 * Start the execution of the {@link JobMaster}.
	 *
	 * @throws Exception if the JobMaster cannot be started
	 */
	void start() throws Exception;

	/**
	 * Get the {@link JobMasterGateway} of the {@link JobMaster}. The future is
	 * only completed if the JobMaster becomes leader.
	 *
	 * @return Future with the JobMasterGateway once the underlying JobMaster becomes leader
	 */
	CompletableFuture<JobMasterGateway> getJobMasterGateway();

	/**
	 * Get the result future of this runner. The future is completed once the executed
	 * job reaches a globally terminal state.
	 *
	 * @return Future which is completed with the job result
	 */
	CompletableFuture<ArchivedExecutionGraph> getResultFuture();

	/**
	 * Get the job id of the executed job.
	 *
	 * @return job id of the executed job
	 */
	JobID getJobID();
}
