package org.apache.flink.runtime.entrypoint;

import org.apache.flink.runtime.entrypoint.parser.ParserResultFactory;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;

import javax.annotation.Nonnull;

import java.util.Properties;

import static org.apache.flink.runtime.entrypoint.parser.CommandLineOptions.CONFIG_DIR_OPTION;
import static org.apache.flink.runtime.entrypoint.parser.CommandLineOptions.DYNAMIC_PROPERTY_OPTION;

/**
 * Parser factory which generates a {@link ClusterConfiguration} from the given
 * list of command line arguments.
 */
public class ClusterConfigurationParserFactory implements ParserResultFactory<ClusterConfiguration> {

	public static Options options() {
		final Options options = new Options();
		options.addOption(CONFIG_DIR_OPTION);
		options.addOption(DYNAMIC_PROPERTY_OPTION);

		return options;
	}

	@Override
	public Options getOptions() {
		return options();
	}

	@Override
	public ClusterConfiguration createResult(@Nonnull CommandLine commandLine) {
		final String configDir = commandLine.getOptionValue(CONFIG_DIR_OPTION.getOpt());

		final Properties dynamicProperties = commandLine.getOptionProperties(DYNAMIC_PROPERTY_OPTION.getOpt());

		return new ClusterConfiguration(configDir, dynamicProperties, commandLine.getArgs());
	}
}
