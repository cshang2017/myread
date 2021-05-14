package org.apache.flink.runtime.jobmanager;

import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;
import org.apache.flink.runtime.jobmaster.JobMaster;

/**
 * Interface for completion actions once a Flink job has reached
 * a terminal state.
 */
public interface OnCompletionActions {

	/**
	 * Job reached a globally terminal state.
	 *
	 * @param executionGraph serializable execution graph
	 */
	void jobReachedGloballyTerminalState(ArchivedExecutionGraph executionGraph);

	/**
	 * Job was finished by another JobMaster.
	 */
	void jobFinishedByOther();

	/**
	 * The {@link JobMaster} failed while executing the job.
	 */
	void jobMasterFailed(Throwable cause);
}
