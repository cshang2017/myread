package org.apache.flink.client.deployment.executors;

import org.apache.flink.annotation.Internal;
import org.apache.flink.client.deployment.StandaloneClientFactory;
import org.apache.flink.client.deployment.StandaloneClusterId;
import org.apache.flink.core.execution.PipelineExecutor;

/**
 * The {@link PipelineExecutor} to be used when executing a job on an already running cluster.
 */
@Internal
public class RemoteExecutor extends AbstractSessionClusterExecutor<StandaloneClusterId, StandaloneClientFactory> {

	public static final String NAME = "remote";

	public RemoteExecutor() {
		super(new StandaloneClientFactory());
	}
}
