package org.apache.flink.client.cli;

import org.apache.commons.cli.CommandLine;

import static org.apache.flink.client.cli.CliFrontendParser.HELP_OPTION;

/**
 * Base class for all options parsed from the command line.
 * Contains options for printing help and the JobManager address.
 */
public abstract class CommandLineOptions {

	private final boolean printHelp;

	protected CommandLineOptions(CommandLine line) {
		this.printHelp = line.hasOption(HELP_OPTION.getOpt());
	}

	public boolean isPrintHelp() {
		return printHelp;
	}
}
