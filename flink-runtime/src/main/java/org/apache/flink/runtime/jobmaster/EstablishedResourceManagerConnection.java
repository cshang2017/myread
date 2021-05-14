package org.apache.flink.runtime.jobmaster;

import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.resourcemanager.ResourceManagerGateway;

import javax.annotation.Nonnull;

/**
 * Class which contains the connection details of an established
 * connection with the ResourceManager.
 */
class EstablishedResourceManagerConnection {

	@Nonnull
	private final ResourceManagerGateway resourceManagerGateway;

	@Nonnull
	private final ResourceID resourceManagerResourceID;

	EstablishedResourceManagerConnection(
			@Nonnull ResourceManagerGateway resourceManagerGateway,
			@Nonnull ResourceID resourceManagerResourceID) {
		this.resourceManagerGateway = resourceManagerGateway;
		this.resourceManagerResourceID = resourceManagerResourceID;
	}

	@Nonnull
	public ResourceManagerGateway getResourceManagerGateway() {
		return resourceManagerGateway;
	}

	@Nonnull
	public ResourceID getResourceManagerResourceID() {
		return resourceManagerResourceID;
	}
}
