package org.apache.flink.runtime.taskexecutor;

import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.entrypoint.ClusterInformation;
import org.apache.flink.runtime.instance.InstanceID;
import org.apache.flink.runtime.registration.RegistrationResponse;
import org.apache.flink.util.Preconditions;

import java.io.Serializable;

/**
 * Base class for responses from the ResourceManager to a registration attempt by a
 * TaskExecutor.
 */
public final class TaskExecutorRegistrationSuccess extends RegistrationResponse.Success implements Serializable {

	private static final long serialVersionUID = 1L;

	private final InstanceID registrationId;

	private final ResourceID resourceManagerResourceId;

	private final ClusterInformation clusterInformation;

	/**
	 * Create a new {@code TaskExecutorRegistrationSuccess} message.
	 *
	 * @param registrationId The ID that the ResourceManager assigned the registration.
	 * @param resourceManagerResourceId The unique ID that identifies the ResourceManager.
	 * @param clusterInformation information about the cluster
	 */
	public TaskExecutorRegistrationSuccess(
			InstanceID registrationId,
			ResourceID resourceManagerResourceId,
			ClusterInformation clusterInformation) {
		this.registrationId = Preconditions.checkNotNull(registrationId);
		this.resourceManagerResourceId = Preconditions.checkNotNull(resourceManagerResourceId);
		this.clusterInformation = Preconditions.checkNotNull(clusterInformation);
	}

	/**
	 * Gets the ID that the ResourceManager assigned the registration.
	 */
	public InstanceID getRegistrationId() {
		return registrationId;
	}

	/**
	 * Gets the unique ID that identifies the ResourceManager.
	 */
	public ResourceID getResourceManagerId() {
		return resourceManagerResourceId;
	}

	/**
	 * Gets the cluster information.
	 */
	public ClusterInformation getClusterInformation() {
		return clusterInformation;
	}


}







