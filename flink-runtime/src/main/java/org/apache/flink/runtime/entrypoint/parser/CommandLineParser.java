package org.apache.flink.runtime.entrypoint.parser;

import org.apache.flink.runtime.entrypoint.FlinkParseException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import javax.annotation.Nonnull;

/**
 * Command line parser which produces a result from the given
 * command line arguments.
 */
public class CommandLineParser<T> {

	@Nonnull
	private final ParserResultFactory<T> parserResultFactory;

	public CommandLineParser(@Nonnull ParserResultFactory<T> parserResultFactory) {
		this.parserResultFactory = parserResultFactory;
	}

	public T parse(@Nonnull String[] args) throws FlinkParseException {
		final DefaultParser parser = new DefaultParser();
		final Options options = parserResultFactory.getOptions();

		final CommandLine commandLine;
			commandLine = parser.parse(options, args, true);
		

		return parserResultFactory.createResult(commandLine);
	}

	public void printHelp(@Nonnull String cmdLineSyntax) {
		final HelpFormatter helpFormatter = new HelpFormatter();
		helpFormatter.setLeftPadding(5);
		helpFormatter.setWidth(80);
		helpFormatter.printHelp(cmdLineSyntax, parserResultFactory.getOptions(), true);
	}
}
