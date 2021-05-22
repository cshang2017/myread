package org.apache.flink.runtime.entrypoint;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.Properties;

/**
 * Basic {@link ClusterConfiguration} for entry points.
 */
public class EntrypointClusterConfiguration extends ClusterConfiguration {

	@Nullable
	private final String hostname;

	private final int restPort;

	public EntrypointClusterConfiguration(@Nonnull String configDir, @Nonnull Properties dynamicProperties, @Nonnull String[] args, @Nullable String hostname, int restPort) {
		super(configDir, dynamicProperties, args);
		this.hostname = hostname;
		this.restPort = restPort;
	}

	public int getRestPort() {
		return restPort;
	}

	@Nullable
	public String getHostname() {
		return hostname;
	}
}
