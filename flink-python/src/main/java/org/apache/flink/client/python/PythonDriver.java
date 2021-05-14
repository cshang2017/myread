package org.apache.flink.client.python;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.entrypoint.parser.CommandLineParser;

import py4j.GatewayServer;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

/**
 * A main class used to launch Python applications. It executes python as a
 * subprocess and then has it connect back to the JVM to access system properties, etc.
 */
public final class PythonDriver {
	public static void main(String[] args)  {
		// The python job needs at least 2 args.
		// e.g. py a.py [user args]
		// e.g. pym a.b [user args]
		if (args.length < 2) {
			System.exit(1);
		}

		// parse args
		CommandLineParser<PythonDriverOptions> commandLineParser = new CommandLineParser<>(
			new PythonDriverOptionsParserFactory());
		PythonDriverOptions pythonDriverOptions = commandLineParser.parse(args);

		// Get configuration from ContextEnvironment/OptimizerPlanEnvironment. As the configurations of
		// streaming and batch environments are always set at the same time, for streaming jobs we can
		// also get its configuration from batch environments.
		Configuration config = ExecutionEnvironment.getExecutionEnvironment().getConfiguration();

		// start gateway server
		GatewayServer gatewayServer = PythonEnvUtils.startGatewayServer();
		PythonEnvUtils.setGatewayServer(gatewayServer);

		// commands which will be exec in python progress.
		final List<String> commands = constructPythonCommands(pythonDriverOptions);
		try {
			// prepare the exec environment of python progress.
			String tmpDir = System.getProperty("java.io.tmpdir") +
				File.separator + "pyflink" + File.separator + UUID.randomUUID();
			// start the python process.
			Process pythonProcess = PythonEnvUtils.launchPy4jPythonClient(
				gatewayServer,
				config,
				commands,
				pythonDriverOptions.getEntryPointScript().orElse(null),
				tmpDir,
				true);
				
			BufferedReader in = new BufferedReader(new InputStreamReader(pythonProcess.getInputStream()));
			
			// print the python process output to stdout and log file
			while (true) {
				String line = in.readLine();
				if (line == null) {
					break;
				} else {
					System.out.println(line);
				}
			}
			int exitCode = pythonProcess.waitFor();
		} finally {
			PythonEnvUtils.setGatewayServer(null);
			gatewayServer.shutdown();
		}
	}

	/**
	 * Constructs the Python commands which will be executed in python process.
	 *
	 * @param pythonDriverOptions parsed Python command options
	 */
	static List<String> constructPythonCommands(final PythonDriverOptions pythonDriverOptions) {
		final List<String> commands = new ArrayList<>();
		if (pythonDriverOptions.getEntryPointScript().isPresent()) {
			commands.add(pythonDriverOptions.getEntryPointScript().get());
		} else {
			commands.add("-m");
			commands.add(pythonDriverOptions.getEntryPointModule());
		}
		commands.addAll(pythonDriverOptions.getProgramArgs());
		return commands;
	}
}
