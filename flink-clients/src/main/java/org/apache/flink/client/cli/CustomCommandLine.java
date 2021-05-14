package org.apache.flink.client.cli;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.FlinkException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;

/**
 * Custom command-line interface to load hooks for the command-line interface.
 */
public interface CustomCommandLine {

	/**
	 * Signals whether the custom command-line wants to execute or not.
	 * @param commandLine The command-line options
	 * @return True if the command-line wants to run, False otherwise
	 */
	boolean isActive(CommandLine commandLine);

	/**
	 * Gets the unique identifier of this CustomCommandLine.
	 * @return A unique identifier
	 */
	String getId();

	/**
	 * Adds custom options to the existing run options.
	 * @param baseOptions The existing options.
	 */
	void addRunOptions(Options baseOptions);

	/**
	 * Adds custom options to the existing general options.
	 *
	 * @param baseOptions The existing options.
	 */
	void addGeneralOptions(Options baseOptions);

	/**
	 * Override configuration settings by specified command line options.
	 *
	 * @param commandLine containing the overriding values
	 * @return the effective configuration with the overridden configuration settings
	 */
	Configuration applyCommandLineOptionsToConfiguration(CommandLine commandLine) throws FlinkException;

	default CommandLine parseCommandLineOptions(String[] args, boolean stopAtNonOptions) throws CliArgsException {
		final Options options = new Options();
		addGeneralOptions(options);
		addRunOptions(options);
		return CliFrontendParser.parse(options, args, stopAtNonOptions);
	}
}
