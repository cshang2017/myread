package org.apache.flink.client.python;

import org.apache.flink.runtime.entrypoint.FlinkParseException;
import org.apache.flink.runtime.entrypoint.parser.ParserResultFactory;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;

import javax.annotation.Nonnull;

import static org.apache.flink.client.cli.CliFrontendParser.PYMODULE_OPTION;
import static org.apache.flink.client.cli.CliFrontendParser.PY_OPTION;

/**
 * Parser factory which generates a {@link PythonDriverOptions} from a given
 * list of command line arguments.
 */
final class PythonDriverOptionsParserFactory implements ParserResultFactory<PythonDriverOptions> {

	@Override
	public Options getOptions() {
		final Options options = new Options();
		options.addOption(PY_OPTION);
		options.addOption(PYMODULE_OPTION);
		return options;
	}

	@Override
	public PythonDriverOptions createResult(@Nonnull CommandLine commandLine) throws FlinkParseException {
		String entryPointModule = null;
		String entryPointScript = null;

		if (commandLine.hasOption(PY_OPTION.getOpt())) {
			entryPointScript = commandLine.getOptionValue(PY_OPTION.getOpt());
		} else if (commandLine.hasOption(PYMODULE_OPTION.getOpt())) {
			entryPointModule = commandLine.getOptionValue(PYMODULE_OPTION.getOpt());
		} 

		return new PythonDriverOptions(
			entryPointModule,
			entryPointScript,
			commandLine.getArgList());
	}
}
