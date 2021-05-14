package org.apache.flink.core.execution;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.Configuration;

import java.util.stream.Stream;

/**
 * An interface to be implemented by the entity responsible for finding the correct {@link PipelineExecutor} to
 * execute a given {@link org.apache.flink.api.dag.Pipeline}.
 */
@Internal
public interface PipelineExecutorServiceLoader {

	/**
	 * Loads the {@link PipelineExecutorFactory} which is compatible with the provided configuration.
	 * There can be at most one compatible factory among the available ones, otherwise an exception
	 * will be thrown.
	 *
	 * @return a compatible {@link PipelineExecutorFactory}.
	 * @throws Exception if there is more than one compatible factories, or something went wrong when
	 * 			loading the registered factories.
	 */
	PipelineExecutorFactory getExecutorFactory(final Configuration configuration) throws Exception;

	/**
	 * Loads and returns a stream of the names of all available executors.
	 */
	Stream<String> getExecutorNames();
}
