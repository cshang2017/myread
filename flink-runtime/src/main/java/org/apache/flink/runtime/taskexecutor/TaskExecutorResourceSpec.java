package org.apache.flink.runtime.taskexecutor;

import org.apache.flink.api.common.resources.CPUResource;
import org.apache.flink.configuration.MemorySize;

/**
 * Specification of resources to use in running {@link org.apache.flink.runtime.taskexecutor.TaskExecutor}.
 */
public class TaskExecutorResourceSpec {
	private final CPUResource cpuCores;

	private final MemorySize taskHeapSize;

	private final MemorySize taskOffHeapSize;

	private final MemorySize networkMemSize;

	private final MemorySize managedMemorySize;

	public TaskExecutorResourceSpec(
			CPUResource cpuCores,
			MemorySize taskHeapSize,
			MemorySize taskOffHeapSize,
			MemorySize networkMemSize,
			MemorySize managedMemorySize) {
		this.cpuCores = cpuCores;
		this.taskHeapSize = taskHeapSize;
		this.taskOffHeapSize = taskOffHeapSize;
		this.networkMemSize = networkMemSize;
		this.managedMemorySize = managedMemorySize;
	}

	public CPUResource getCpuCores() {
		return cpuCores;
	}

	public MemorySize getTaskHeapSize() {
		return taskHeapSize;
	}

	public MemorySize getTaskOffHeapSize() {
		return taskOffHeapSize;
	}

	public MemorySize getNetworkMemSize() {
		return networkMemSize;
	}

	public MemorySize getManagedMemorySize() {
		return managedMemorySize;
	}
}
