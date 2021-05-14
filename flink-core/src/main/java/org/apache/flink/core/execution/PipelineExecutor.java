package org.apache.flink.core.execution;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.dag.Pipeline;
import org.apache.flink.configuration.Configuration;

import java.util.concurrent.CompletableFuture;

/**
 * The entity responsible for executing a {@link Pipeline}, i.e. a user job.
 */
@Internal
public interface PipelineExecutor {

	/**
	 * Executes a {@link Pipeline} based on the provided configuration and returns a {@link JobClient} which allows to
	 * interact with the job being executed, e.g. cancel it or take a savepoint.
	 *
	 * <p><b>ATTENTION:</b> The caller is responsible for managing the lifecycle of the returned {@link JobClient}. This
	 * means that e.g. {@code close()} should be called explicitly at the call-site.
	 *
	 * @param pipeline the {@link Pipeline} to execute
	 * @param configuration the {@link Configuration} with the required execution parameters
	 * @return a {@link CompletableFuture} with the {@link JobClient} corresponding to the pipeline.
	 */
	CompletableFuture<JobClient> execute(final Pipeline pipeline, final Configuration configuration) throws Exception;
}
