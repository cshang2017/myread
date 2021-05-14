

package org.apache.flink.api.java.utils;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.core.execution.PipelineExecutor;
import org.apache.flink.core.execution.PipelineExecutorFactory;

/**
 * An {@link PipelineExecutorFactory} for {@link CollectionPipelineExecutor}.
 */
@Internal
public class CollectionExecutorFactory implements PipelineExecutorFactory {

	@Override
	public String getName() {
		return CollectionPipelineExecutor.NAME;
	}

	@Override
	public boolean isCompatibleWith(Configuration configuration) {
		return CollectionPipelineExecutor.NAME.equalsIgnoreCase(configuration.get(DeploymentOptions.TARGET));
	}

	@Override
	public PipelineExecutor getExecutor(Configuration configuration) {
		return new CollectionPipelineExecutor();
	}
}
