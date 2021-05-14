
package org.apache.flink.client.cli;

import org.apache.commons.cli.CommandLine;

import static org.apache.flink.client.cli.CliFrontendParser.CANCEL_WITH_SAVEPOINT_OPTION;

/**
 * Command line options for the CANCEL command.
 */
public class CancelOptions extends CommandLineOptions {

	private final String[] args;

	/** Flag indicating whether to cancel with a savepoint. */
	private final boolean withSavepoint;

	/** Optional target directory for the savepoint. Overwrites cluster default. */
	private final String targetDirectory;

	public CancelOptions(CommandLine line) {
		super(line);
		this.args = line.getArgs();
		this.withSavepoint = line.hasOption(CANCEL_WITH_SAVEPOINT_OPTION.getOpt());
		this.targetDirectory = line.getOptionValue(CANCEL_WITH_SAVEPOINT_OPTION.getOpt());
	}

	public String[] getArgs() {
		return args == null ? new String[0] : args;
	}

	public boolean isWithSavepoint() {
		return withSavepoint;
	}

	public String getSavepointTargetDirectory() {
		return targetDirectory;
	}
}
