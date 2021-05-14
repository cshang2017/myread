package org.apache.flink.runtime.taskexecutor;

import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.instance.InstanceID;
import org.apache.flink.runtime.resourcemanager.ResourceManagerGateway;

import javax.annotation.Nonnull;

/**
 * Container for the resource manager connection instances used by the
 * {@link TaskExecutor}.
 */
class EstablishedResourceManagerConnection {

	@Nonnull
	private final ResourceManagerGateway resourceManagerGateway;

	@Nonnull
	private final ResourceID resourceManagerResourceId;

	@Nonnull
	private final InstanceID taskExecutorRegistrationId;

	EstablishedResourceManagerConnection(@Nonnull ResourceManagerGateway resourceManagerGateway, @Nonnull ResourceID resourceManagerResourceId, @Nonnull InstanceID taskExecutorRegistrationId) {
		this.resourceManagerGateway = resourceManagerGateway;
		this.resourceManagerResourceId = resourceManagerResourceId;
		this.taskExecutorRegistrationId = taskExecutorRegistrationId;
	}

	@Nonnull
	public ResourceManagerGateway getResourceManagerGateway() {
		return resourceManagerGateway;
	}

	@Nonnull
	public ResourceID getResourceManagerResourceId() {
		return resourceManagerResourceId;
	}

	@Nonnull
	public InstanceID getTaskExecutorRegistrationId() {
		return taskExecutorRegistrationId;
	}
}
