package org.apache.flink.runtime.resourcemanager.registration;

import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.instance.InstanceID;
import org.apache.flink.runtime.taskexecutor.TaskExecutorGateway;

import lombok.AllArgsConstructor;
import lombok.Getter;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * This class is responsible for grouping the TaskExecutorGateway and the InstanceID
 * of a registered task executor.
 */
@Getter
public class TaskExecutorConnection {

	private final ResourceID resourceID;
	private final InstanceID instanceID;
	private final TaskExecutorGateway taskExecutorGateway;

	public TaskExecutorConnection(ResourceID resourceID, TaskExecutorGateway taskExecutorGateway) {
		this.resourceID = checkNotNull(resourceID);
		this.instanceID = new InstanceID();
		this.taskExecutorGateway = checkNotNull(taskExecutorGateway);
	}
}
