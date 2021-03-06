package org.apache.flink.client.cli;

import org.apache.commons.cli.CommandLine;

import static org.apache.flink.client.cli.CliFrontendParser.JAR_OPTION;
import static org.apache.flink.client.cli.CliFrontendParser.SAVEPOINT_DISPOSE_OPTION;

/**
 * Command line options for the SAVEPOINT command.
 */
public class SavepointOptions extends CommandLineOptions {

	private final String[] args;
	private boolean dispose;
	private String disposeSavepointPath;
	private String jarFile;

	public SavepointOptions(CommandLine line) {
		super(line);
		args = line.getArgs();
		dispose = line.hasOption(SAVEPOINT_DISPOSE_OPTION.getOpt());
		disposeSavepointPath = line.getOptionValue(SAVEPOINT_DISPOSE_OPTION.getOpt());
		jarFile = line.getOptionValue(JAR_OPTION.getOpt());
	}

	public String[] getArgs() {
		return args == null ? new String[0] : args;
	}

	public boolean isDispose() {
		return dispose;
	}

	public String getSavepointPath() {
		return disposeSavepointPath;
	}

	public String getJarFilePath() {
		return jarFile;
	}
}
