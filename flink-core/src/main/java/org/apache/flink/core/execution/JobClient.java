package org.apache.flink.core.execution;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;

import javax.annotation.Nullable;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * A client that is scoped to a specific job.
 */
@PublicEvolving
public interface JobClient {

	/**
	 * Returns the {@link JobID} that uniquely identifies the job this client is scoped to.
	 */
	JobID getJobID();

	/**
	 * Requests the {@link JobStatus} of the associated job.
	 */
	CompletableFuture<JobStatus> getJobStatus();

	/**
	 * Cancels the associated job.
	 */
	CompletableFuture<Void> cancel();

	/**
	 * Stops the associated job on Flink cluster.
	 *
	 * <p>Stopping works only for streaming programs. Be aware, that the job might continue to run for
	 * a while after sending the stop command, because after sources stopped to emit data all operators
	 * need to finish processing.
	 *
	 * @param advanceToEndOfEventTime flag indicating if the source should inject a {@code MAX_WATERMARK} in the pipeline
	 * @param savepointDirectory directory the savepoint should be written to
	 * @return a {@link CompletableFuture} containing the path where the savepoint is located
	 */
	CompletableFuture<String> stopWithSavepoint(boolean advanceToEndOfEventTime, @Nullable String savepointDirectory);

	/**
	 * Triggers a savepoint for the associated job. The savepoint will be written to the given savepoint directory,
	 * or {@link org.apache.flink.configuration.CheckpointingOptions#SAVEPOINT_DIRECTORY} if it is null.
	 *
	 * @param savepointDirectory directory the savepoint should be written to
	 * @return a {@link CompletableFuture} containing the path where the savepoint is located
	 */
	CompletableFuture<String> triggerSavepoint(@Nullable String savepointDirectory);

	/**
	 * Requests the accumulators of the associated job. Accumulators can be requested while it is running
	 * or after it has finished. The class loader is used to deserialize the incoming accumulator results.
	 */
	CompletableFuture<Map<String, Object>> getAccumulators(ClassLoader classLoader);

	/**
	 * Returns the {@link JobExecutionResult result of the job execution} of the submitted job.
	 *
	 * @param userClassloader the classloader used to de-serialize the accumulators of the job.
	 */
	CompletableFuture<JobExecutionResult> getJobExecutionResult(final ClassLoader userClassloader);
}
