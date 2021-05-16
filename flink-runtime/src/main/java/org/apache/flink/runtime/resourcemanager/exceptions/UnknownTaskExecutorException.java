package org.apache.flink.runtime.resourcemanager.exceptions;

import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.taskexecutor.TaskExecutor;

public class UnknownTaskExecutorException extends ResourceManagerException {

	public UnknownTaskExecutorException(ResourceID taskExecutorId) {
		super("No TaskExecutor registered under " + taskExecutorId + '.');
	}
}
