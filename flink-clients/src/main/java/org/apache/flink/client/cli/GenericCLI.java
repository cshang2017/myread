package org.apache.flink.client.cli;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.configuration.DeploymentOptionsInternal;
import org.apache.flink.configuration.UnmodifiableConfiguration;
import org.apache.flink.core.execution.DefaultExecutorServiceLoader;
import org.apache.flink.core.execution.PipelineExecutor;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A generic implementation of the {@link CustomCommandLine} that only expects
 * the execution.target parameter to be explicitly specified and simply forwards the
 * rest of the options specified with -D to the corresponding {@link PipelineExecutor}
 * for further parsing.
 */
@Internal
public class GenericCLI implements CustomCommandLine {


	private static final String ID = "Generic CLI";

	private final Option executorOption = new Option("e", "executor", true,
			"DEPRECATED: Please use the -t option instead which is also available with the \"Application Mode\".\n" +
					"The name of the executor to be used for executing the given job, which is equivalent " +
					"to the \"" + DeploymentOptions.TARGET.key() + "\" config option. The " +
					"currently available executors are: " + getExecutorFactoryNames() + ".");

	private final Option targetOption = new Option("t", "target", true,
			"The deployment target for the given application, which is equivalent " +
					"to the \"" + DeploymentOptions.TARGET.key() + "\" config option. The " +
					"currently available targets are: " + getExecutorFactoryNames() +
					", \"yarn-application\" and \"kubernetes-application\".");

	/**
	 * Dynamic properties allow the user to specify additional configuration values with -D, such as
	 * <tt> -Dfs.overwrite-files=true  -Dtaskmanager.memory.network.min=536346624</tt>.
	 */
	private final Option dynamicProperties = Option.builder("D")
			.argName("property=value")
			.numberOfArgs(2)
			.valueSeparator('=')
			.desc("Generic configuration options for execution/deployment and for the configured " +
					"executor. The available options can be found at " +
					"https://ci.apache.org/projects/flink/flink-docs-stable/ops/config.html")
			.build();

	private final Configuration baseConfiguration;

	private final String configurationDir;

	public GenericCLI(final Configuration configuration, final String configDir) {
		this.baseConfiguration = new UnmodifiableConfiguration(checkNotNull(configuration));
		this.configurationDir =  checkNotNull(configDir);
	}

	@Override
	public boolean isActive(CommandLine commandLine) {
		return baseConfiguration.getOptional(DeploymentOptions.TARGET).isPresent()
				|| commandLine.hasOption(executorOption.getOpt())
				|| commandLine.hasOption(targetOption.getOpt());
	}

	@Override
	public String getId() {
		return ID;
	}

	@Override
	public void addRunOptions(Options baseOptions) {
		// nothing to add here
	}

	@Override
	public void addGeneralOptions(Options baseOptions) {
		baseOptions.addOption(executorOption);
		baseOptions.addOption(targetOption);
		baseOptions.addOption(dynamicProperties);
	}

	@Override
	public Configuration applyCommandLineOptionsToConfiguration(final CommandLine commandLine) {
		final Configuration effectiveConfiguration = new Configuration(baseConfiguration);

		final String executorName = commandLine.getOptionValue(executorOption.getOpt());
		if (executorName != null) {
			effectiveConfiguration.setString(DeploymentOptions.TARGET, executorName);
		}

		final String targetName = commandLine.getOptionValue(targetOption.getOpt());
		if (targetName != null) {
			effectiveConfiguration.setString(DeploymentOptions.TARGET, targetName);
		}

		encodeDynamicProperties(commandLine, effectiveConfiguration);
		effectiveConfiguration.set(DeploymentOptionsInternal.CONF_DIR, configurationDir);

			return effectiveConfiguration;
	}

	private void encodeDynamicProperties(final CommandLine commandLine, final Configuration effectiveConfiguration) {
		final Properties properties = commandLine.getOptionProperties(dynamicProperties.getOpt());
		properties.stringPropertyNames()
				.forEach(key -> {
					final String value = properties.getProperty(key);
					if (value != null) {
						effectiveConfiguration.setString(key, value);
					} else {
						effectiveConfiguration.setString(key, "true");
					}
				});
	}

	private static String getExecutorFactoryNames() {
		return new DefaultExecutorServiceLoader().getExecutorNames()
				.map(name -> String.format("\"%s\"", name))
				.collect(Collectors.joining(", "));
	}
}
