package org.apache.flink.runtime.entrypoint;

import org.apache.flink.runtime.entrypoint.parser.ParserResultFactory;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;

import javax.annotation.Nonnull;

import java.util.Properties;

import static org.apache.flink.runtime.entrypoint.parser.CommandLineOptions.CONFIG_DIR_OPTION;
import static org.apache.flink.runtime.entrypoint.parser.CommandLineOptions.DYNAMIC_PROPERTY_OPTION;
import static org.apache.flink.runtime.entrypoint.parser.CommandLineOptions.EXECUTION_MODE_OPTION;
import static org.apache.flink.runtime.entrypoint.parser.CommandLineOptions.HOST_OPTION;
import static org.apache.flink.runtime.entrypoint.parser.CommandLineOptions.REST_PORT_OPTION;

/**
 * Parser factory for {@link EntrypointClusterConfiguration}.
 */
public class EntrypointClusterConfigurationParserFactory implements ParserResultFactory<EntrypointClusterConfiguration> {

	@Override
	public Options getOptions() {
		final Options options = new Options();
		options.addOption(CONFIG_DIR_OPTION);
		options.addOption(REST_PORT_OPTION);
		options.addOption(DYNAMIC_PROPERTY_OPTION);
		options.addOption(HOST_OPTION);
		options.addOption(EXECUTION_MODE_OPTION);

		return options;
	}

	@Override
	public EntrypointClusterConfiguration createResult(@Nonnull CommandLine commandLine) {
		final String configDir = commandLine.getOptionValue(CONFIG_DIR_OPTION.getOpt());
		final Properties dynamicProperties = commandLine.getOptionProperties(DYNAMIC_PROPERTY_OPTION.getOpt());
		final String restPortStr = commandLine.getOptionValue(REST_PORT_OPTION.getOpt(), "-1");
		final int restPort = Integer.parseInt(restPortStr);
		final String hostname = commandLine.getOptionValue(HOST_OPTION.getOpt());

		return new EntrypointClusterConfiguration(
			configDir,
			dynamicProperties,
			commandLine.getArgs(),
			hostname,
			restPort);
	}
}
