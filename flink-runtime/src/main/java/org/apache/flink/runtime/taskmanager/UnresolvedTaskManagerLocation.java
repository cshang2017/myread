package org.apache.flink.runtime.taskmanager;

import org.apache.flink.runtime.clusterframework.types.ResourceID;

import java.io.Serializable;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * This class encapsulates the connection information of a TaskManager, without resolving the hostname.
 * See also {@link TaskManagerLocation}.
 */
public class UnresolvedTaskManagerLocation implements Serializable {

	private final ResourceID resourceID;
	private final String externalAddress;
	private final int dataPort;

	public UnresolvedTaskManagerLocation(final ResourceID resourceID, final String externalAddress, final int dataPort) {
		// -1 indicates a local instance connection info
		checkArgument(dataPort > 0 || dataPort == -1, "dataPort must be > 0, or -1 (local)");

		this.resourceID = checkNotNull(resourceID);
		this.externalAddress = checkNotNull(externalAddress);
		this.dataPort = dataPort;
	}

	public ResourceID getResourceID() {
		return resourceID;
	}

	public String getExternalAddress() {
		return externalAddress;
	}

	public int getDataPort() {
		return dataPort;
	}
}
