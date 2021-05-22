package org.apache.flink.runtime.entrypoint;

import org.apache.flink.util.Preconditions;

import java.io.Serializable;

/**
 * Information about the cluster which is shared with the cluster components.
 */
public class ClusterInformation implements Serializable {

	private final String blobServerHostname;

	private final int blobServerPort;

	public ClusterInformation(String blobServerHostname, int blobServerPort) {
		this.blobServerHostname = Preconditions.checkNotNull(blobServerHostname);
		Preconditions.checkArgument(
			0 < blobServerPort && blobServerPort < 65_536,
			"The blob port must between 0 and 65_536. However, it was " + blobServerPort + '.');
		this.blobServerPort = blobServerPort;
	}

	public String getBlobServerHostname() {
		return blobServerHostname;
	}

	public int getBlobServerPort() {
		return blobServerPort;
	}

}
