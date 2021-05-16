
package org.apache.flink.runtime.resourcemanager;

import org.apache.flink.runtime.clusterframework.types.ResourceProfile;

import java.util.UUID;
import java.util.concurrent.Executor;

/**
 * Interface which provides access to services of the ResourceManager.
 */
public interface ResourceManagerServices {

	/**
	 * Gets the current leader id assigned at the ResourceManager.
	 */
	UUID getLeaderID();

	/**
	 * Allocates a resource according to the resource profile.
	 */
	void allocateResource(ResourceProfile resourceProfile);

	/**
	 * Gets the async executor which executes outside of the main thread of the ResourceManager
	 */
	Executor getAsyncExecutor();

	/**
	 * Gets the executor which executes in the main thread of the ResourceManager
	 */
	Executor getMainThreadExecutor();

}
