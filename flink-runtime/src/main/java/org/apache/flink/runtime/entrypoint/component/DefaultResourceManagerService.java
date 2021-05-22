package org.apache.flink.runtime.entrypoint.component;

import org.apache.flink.runtime.resourcemanager.ResourceManager;
import org.apache.flink.runtime.resourcemanager.ResourceManagerGateway;

import java.util.concurrent.CompletableFuture;

/**
 * Default {@link DispatcherResourceManagerComponent.ResourceManagerService} implementation which
 * uses a {@link ResourceManager} instance.
 */
public class DefaultResourceManagerService implements DispatcherResourceManagerComponent.ResourceManagerService {

	private final ResourceManager<?> resourceManager;

	private DefaultResourceManagerService(ResourceManager<?> resourceManager) {
		this.resourceManager = resourceManager;
	}

	@Override
	public ResourceManagerGateway getGateway() {
		return resourceManager.getSelfGateway(ResourceManagerGateway.class);
	}

	@Override
	public CompletableFuture<Void> getTerminationFuture() {
		return resourceManager.getTerminationFuture();
	}

	@Override
	public CompletableFuture<Void> closeAsync() {
		return resourceManager.closeAsync();
	}

	public static DefaultResourceManagerService createFor(ResourceManager<?> resourceManager) {
		return new DefaultResourceManagerService(resourceManager);
	}
}
