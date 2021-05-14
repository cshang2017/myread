package org.apache.flink.client.cli;

import org.apache.commons.cli.CommandLine;

import static org.apache.flink.client.cli.CliFrontendParser.STOP_AND_DRAIN;
import static org.apache.flink.client.cli.CliFrontendParser.STOP_WITH_SAVEPOINT_PATH;

/**
 * Command line options for the STOP command.
 */
class StopOptions extends CommandLineOptions {

	private final String[] args;

	private final boolean savepointFlag;

	/** Optional target directory for the savepoint. Overwrites cluster default. */
	private final String targetDirectory;

	private final boolean advanceToEndOfEventTime;

	StopOptions(CommandLine line) {
		super(line);
		this.args = line.getArgs();

		this.savepointFlag = line.hasOption(STOP_WITH_SAVEPOINT_PATH.getOpt());
		this.targetDirectory = line.getOptionValue(STOP_WITH_SAVEPOINT_PATH.getOpt());

		this.advanceToEndOfEventTime = line.hasOption(STOP_AND_DRAIN.getOpt());
	}

	String[] getArgs() {
		return args == null ? new String[0] : args;
	}

	boolean hasSavepointFlag() {
		return savepointFlag;
	}

	String getTargetDirectory() {
		return targetDirectory;
	}

	boolean shouldAdvanceToEndOfEventTime() {
		return advanceToEndOfEventTime;
	}
}
