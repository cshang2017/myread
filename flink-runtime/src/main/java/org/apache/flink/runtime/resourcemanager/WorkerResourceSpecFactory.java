package org.apache.flink.runtime.resourcemanager;

import org.apache.flink.api.common.resources.CPUResource;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.clusterframework.TaskExecutorProcessSpec;
import org.apache.flink.runtime.clusterframework.TaskExecutorProcessUtils;

/**
 * Factory for creating deployment specific default {@link WorkerResourceSpec}.
 */
public abstract class WorkerResourceSpecFactory {

	public abstract WorkerResourceSpec createDefaultWorkerResourceSpec(Configuration configuration);

	protected WorkerResourceSpec workerResourceSpecFromConfigAndCpu(Configuration configuration, CPUResource cpuResource) {
		final TaskExecutorProcessSpec taskExecutorProcessSpec = TaskExecutorProcessUtils
			.newProcessSpecBuilder(configuration)
			.withCpuCores(cpuResource)
			.build();
		return WorkerResourceSpec.fromTaskExecutorProcessSpec(taskExecutorProcessSpec);
	}
}
