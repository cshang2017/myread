package org.apache.flink.runtime.entrypoint;

import javax.annotation.Nonnull;

import java.util.Properties;

/**
 * Configuration class which contains the parsed command line arguments for
 * the {@link ClusterEntrypoint}.
 */
public class ClusterConfiguration {

	@Nonnull
	private final String configDir;

	@Nonnull
	private final Properties dynamicProperties;

	@Nonnull
	private final String[] args;

	public ClusterConfiguration(@Nonnull String configDir, @Nonnull Properties dynamicProperties, @Nonnull String[] args) {
		this.configDir = configDir;
		this.dynamicProperties = dynamicProperties;
		this.args = args;
	}

	@Nonnull
	public String getConfigDir() {
		return configDir;
	}

	@Nonnull
	public Properties getDynamicProperties() {
		return dynamicProperties;
	}

	@Nonnull
	public String[] getArgs() {
		return args;
	}
}
