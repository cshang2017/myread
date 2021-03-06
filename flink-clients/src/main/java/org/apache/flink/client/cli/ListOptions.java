package org.apache.flink.client.cli;

import org.apache.commons.cli.CommandLine;

import static org.apache.flink.client.cli.CliFrontendParser.ALL_OPTION;
import static org.apache.flink.client.cli.CliFrontendParser.RUNNING_OPTION;
import static org.apache.flink.client.cli.CliFrontendParser.SCHEDULED_OPTION;

/**
 * Command line options for the LIST command.
 */
public class ListOptions extends CommandLineOptions {

	private final boolean showRunning;
	private final boolean showScheduled;
	private final boolean showAll;

	public ListOptions(CommandLine line) {
		super(line);
		this.showAll = line.hasOption(ALL_OPTION.getOpt());
		this.showRunning = line.hasOption(RUNNING_OPTION.getOpt());
		this.showScheduled = line.hasOption(SCHEDULED_OPTION.getOpt());
	}

	public boolean showRunning() {
		return showRunning;
	}

	public boolean showScheduled() {
		return showScheduled;
	}

	public boolean showAll() {
		return showAll;
	}
}
