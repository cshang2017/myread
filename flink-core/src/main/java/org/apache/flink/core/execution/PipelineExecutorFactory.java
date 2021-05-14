package org.apache.flink.core.execution;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.Configuration;

/**
 * A factory for selecting and instantiating the adequate {@link PipelineExecutor}
 * based on a provided {@link Configuration}.
 */
@Internal
public interface PipelineExecutorFactory {

	/**
	 * Returns the name of the executor that this factory creates.
	 */
	String getName();

	/**
	 * Returns {@code true} if this factory is compatible with the options in the
	 * provided configuration, {@code false} otherwise.
	 */
	boolean isCompatibleWith(final Configuration configuration);

	/**
	 * Instantiates an {@link PipelineExecutor} compatible with the provided configuration.
	 * @return the executor instance.
	 */
	PipelineExecutor getExecutor(final Configuration configuration);
}
