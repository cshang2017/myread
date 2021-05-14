package org.apache.flink.client.cli;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.ConfigUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Accessor that exposes config settings that are relevant for execution from an underlying {@link Configuration}.
 */
@Internal
public class ExecutionConfigAccessor {

	private final Configuration configuration;

	private ExecutionConfigAccessor(final Configuration configuration) {
		this.configuration = checkNotNull(configuration);
	}

	/**
	 * Creates an {@link ExecutionConfigAccessor} based on the provided {@link Configuration}.
	 */
	public static ExecutionConfigAccessor fromConfiguration(final Configuration configuration) {
		return new ExecutionConfigAccessor(checkNotNull(configuration));
	}

	/**
	 * Creates an {@link ExecutionConfigAccessor} based on the provided {@link ProgramOptions} as provided by the user through the CLI.
	 */
	public static <T> ExecutionConfigAccessor fromProgramOptions(final ProgramOptions options, final List<T> jobJars) {
		checkNotNull(options);
		checkNotNull(jobJars);

		final Configuration configuration = new Configuration();

		options.applyToConfiguration(configuration);
		ConfigUtils.encodeCollectionToConfig(configuration, PipelineOptions.JARS, jobJars, Object::toString);

		return new ExecutionConfigAccessor(configuration);
	}

	public Configuration applyToConfiguration(final Configuration baseConfiguration) {
		baseConfiguration.addAll(configuration);
		return baseConfiguration;
	}

	public List<URL> getJars() throws MalformedURLException {
		return ConfigUtils.decodeListFromConfig(configuration, PipelineOptions.JARS, URL::new);
	}

	public List<URL> getClasspaths() throws MalformedURLException {
		return ConfigUtils.decodeListFromConfig(configuration, PipelineOptions.CLASSPATHS, URL::new);
	}

	public int getParallelism() {
		return  configuration.getInteger(CoreOptions.DEFAULT_PARALLELISM);
	}

	public boolean getDetachedMode() {
		return !configuration.getBoolean(DeploymentOptions.ATTACHED);
	}

	public SavepointRestoreSettings getSavepointRestoreSettings() {
		return SavepointRestoreSettings.fromConfiguration(configuration);
	}

	public boolean isShutdownOnAttachedExit() {
		return configuration.getBoolean(DeploymentOptions.SHUTDOWN_IF_ATTACHED);
	}
}
