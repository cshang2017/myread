package org.apache.flink.runtime.query;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.jobmaster.KvStateLocationOracle;

import javax.annotation.Nullable;

/**
 * An interface for the Queryable State Client Proxy running on each Task Manager in the cluster.
 *
 * <p>This proxy is where the Queryable State Client (potentially running outside your Flink
 * cluster) connects to, and his responsibility is to forward the client's requests to the rest
 * of the entities participating in fetching the requested state, and running within the cluster.
 *
 * <p>These are:
 * <ol>
 *     <li> the {@link org.apache.flink.runtime.jobmaster.JobMaster Job Manager},
 *     which is responsible for sending the
 *     {@link org.apache.flink.runtime.taskexecutor.TaskExecutor Task Manager} storing
 *     the requested state, and </li>
 *     <li> the Task Manager having the state itself.</li>
 * </ol>
 */
public interface KvStateClientProxy extends KvStateServer {

	/**
	 * Updates the active {@link org.apache.flink.runtime.jobmaster.JobMaster Job Manager}
	 * in case of change.
	 *
	 * <p>This is useful in settings where high-availability is enabled and
	 * a failed Job Manager is replaced by a new one.
	 *
	 * <p><b>IMPORTANT: </b> this method may be called by a different thread than
	 * the {@link #getKvStateLocationOracle(JobID)}.
	 *
	 * @param jobId identifying the job for which to update the key-value state location oracle
	 * @param kvStateLocationOracle the key-value state location oracle for the given {@link JobID},
	 *                                 or null if there is no oracle anymore
	 * */
	void updateKvStateLocationOracle(
		JobID jobId,
		@Nullable KvStateLocationOracle kvStateLocationOracle);

	/**
	 * Retrieves a future containing the currently leading key-value state location oracle.
	 *
	 * <p><b>IMPORTANT: </b> this method may be called by a different thread than the
	 * {@link #updateKvStateLocationOracle(JobID, KvStateLocationOracle)}.
	 *
	 * @param jobId identifying the job for which to request the key-value state location oracle
	 * @return The key-value state location oracle for the given {@link JobID} or null if none.
	 */
	@Nullable KvStateLocationOracle getKvStateLocationOracle(JobID jobId);
}
