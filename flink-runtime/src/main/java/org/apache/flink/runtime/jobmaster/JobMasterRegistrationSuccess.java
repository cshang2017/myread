package org.apache.flink.runtime.jobmaster;

import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.registration.RegistrationResponse;
import org.apache.flink.runtime.resourcemanager.ResourceManagerId;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Base class for responses from the ResourceManager to a registration attempt by a JobMaster.
 */
public class JobMasterRegistrationSuccess extends RegistrationResponse.Success {

	private final ResourceManagerId resourceManagerId;

	private final ResourceID resourceManagerResourceId;

	public JobMasterRegistrationSuccess(
			final ResourceManagerId resourceManagerId,
			final ResourceID resourceManagerResourceId) {
		this.resourceManagerId = checkNotNull(resourceManagerId);
		this.resourceManagerResourceId = checkNotNull(resourceManagerResourceId);
	}

	public ResourceManagerId getResourceManagerId() {
		return resourceManagerId;
	}

	public ResourceID getResourceManagerResourceId() {
		return resourceManagerResourceId;
	}

}
