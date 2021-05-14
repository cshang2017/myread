package org.apache.flink.client.deployment.executors;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.core.execution.PipelineExecutor;
import org.apache.flink.core.execution.PipelineExecutorFactory;

/**
 * An {@link PipelineExecutorFactory} for {@link LocalExecutor local executors}.
 */
@Internal
public class LocalExecutorFactory implements PipelineExecutorFactory {

	@Override
	public String getName() {
		return LocalExecutor.NAME;
	}

	@Override
	public boolean isCompatibleWith(final Configuration configuration) {
		return LocalExecutor.NAME.equalsIgnoreCase(configuration.get(DeploymentOptions.TARGET));
	}

	@Override
	public PipelineExecutor getExecutor(final Configuration configuration) {
		return LocalExecutor.create(configuration);
	}
}
