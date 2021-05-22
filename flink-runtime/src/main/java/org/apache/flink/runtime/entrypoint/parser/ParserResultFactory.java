
package org.apache.flink.runtime.entrypoint.parser;

import org.apache.flink.runtime.entrypoint.FlinkParseException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;

import javax.annotation.Nonnull;

/**
 * Parser result factory used by the {@link CommandLineParser}.
 *
 * @param <T> type of the parsed result
 */
public interface ParserResultFactory<T> {

	/**
	 * Returns all relevant {@link Options} for parsing the command line
	 * arguments.
	 *
	 * @return Options to use for the parsing
	 */
	Options getOptions();

	/**
	 * Create the result of the command line argument parsing.
	 *
	 * @param commandLine to extract the options from
	 * @return Result of the parsing
	 * @throws FlinkParseException Thrown on failures while parsing command line arguments
	 */
	T createResult(@Nonnull CommandLine commandLine) throws FlinkParseException;
}
