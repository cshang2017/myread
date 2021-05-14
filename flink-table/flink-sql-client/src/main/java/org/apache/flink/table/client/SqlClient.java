package org.apache.flink.table.client;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.client.cli.CliClient;
import org.apache.flink.table.client.cli.CliOptions;
import org.apache.flink.table.client.cli.CliOptionsParser;
import org.apache.flink.table.client.config.Environment;
import org.apache.flink.table.client.gateway.Executor;
import org.apache.flink.table.client.gateway.SessionContext;
import org.apache.flink.table.client.gateway.local.LocalExecutor;

import org.apache.commons.lang3.SystemUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.table.client.config.entries.ConfigurationEntry.create;
import static org.apache.flink.table.client.config.entries.ConfigurationEntry.merge;

/**
 * SQL Client for submitting SQL statements. The client can be executed in two
 * modes: a gateway and embedded mode.
 *
 * <p>- In embedded mode, the SQL CLI is tightly coupled with the executor in a common process. This
 * allows for submitting jobs without having to start an additional component.
 *
 * <p>- In future versions: In gateway mode, the SQL CLI client connects to the REST API of the gateway
 * and allows for managing queries via console.
 *
 * <p>For debugging in an IDE you can execute the main method of this class using:
 * "embedded --defaults /path/to/sql-client-defaults.yaml --jar /path/to/target/flink-sql-client-*.jar"
 *
 * <p>Make sure that the FLINK_CONF_DIR environment variable is set.
 */
public class SqlClient {


	private final boolean isEmbedded;
	private final CliOptions options;

	public static final String MODE_EMBEDDED = "embedded";
	public static final String MODE_GATEWAY = "gateway";

	public static final String DEFAULT_SESSION_ID = "default";

	public SqlClient(boolean isEmbedded, CliOptions options) {
		this.isEmbedded = isEmbedded;
		this.options = options;
	}

	private void start() {
		if (isEmbedded) {
			// create local executor with default environment
			final List<URL> jars;
			if (options.getJars() != null) {
				jars = options.getJars();
			} else {
				jars = Collections.emptyList();
			}
			final List<URL> libDirs;
			if (options.getLibraryDirs() != null) {
				libDirs = options.getLibraryDirs();
			} else {
				libDirs = Collections.emptyList();
			}
			final Executor executor = new LocalExecutor(options.getDefaults(), jars, libDirs);
			executor.start();

			// create CLI client with session environment
			final Environment sessionEnv = readSessionEnvironment(options.getEnvironment());
			appendPythonConfig(sessionEnv, options.getPythonConfiguration());
			final SessionContext context;
			if (options.getSessionId() == null) {
				context = new SessionContext(DEFAULT_SESSION_ID, sessionEnv);
			} else {
				context = new SessionContext(options.getSessionId(), sessionEnv);
			}

			// Open an new session
			String sessionId = executor.openSession(context);
			try {
				// add shutdown hook
				Runtime.getRuntime().addShutdownHook(new EmbeddedShutdownThread(sessionId, executor));

				// do the actual work
				openCli(sessionId, executor);
			} finally {
				executor.closeSession(sessionId);
			}
		} 
	}

	/**
	 * Opens the CLI client for executing SQL statements.
	 *
	 * @param sessionId session identifier for the current client.
	 * @param executor executor
	 */
	private void openCli(String sessionId, Executor executor) {
		CliClient cli = null;
		try {
			Path historyFilePath;
			if (options.getHistoryFilePath() != null) {
				historyFilePath = Paths.get(options.getHistoryFilePath());
			} else {
				historyFilePath = Paths.get(System.getProperty("user.home"),
						SystemUtils.IS_OS_WINDOWS ? "flink-sql-history" : ".flink-sql-history");
			}
			cli = new CliClient(sessionId, executor, historyFilePath);
			// interactive CLI mode
			if (options.getUpdateStatement() == null) {
				cli.open();
			}
			// execute single update statement
			else {
				final boolean success = cli.submitUpdate(options.getUpdateStatement());
			}
		} finally {
			if (cli != null) {
				cli.close();
			}
		}
	}

	// --------------------------------------------------------------------------------------------

	private static Environment readSessionEnvironment(URL envUrl) {
		// use an empty environment by default
		if (envUrl == null) {
			System.out.println("No session environment specified.");
			return new Environment();
		}

		System.out.println("Reading session environment from: " + envUrl);
		LOG.info("Using session environment file: {}", envUrl);
		try {
			return Environment.parse(envUrl);
		} catch (IOException e) {
			throw new SqlClientException("Could not read session environment file at: " + envUrl, e);
		}
	}

	private static void appendPythonConfig(Environment env, Configuration pythonConfiguration) {
		Map<String, Object> pythonConfig = new HashMap<>(pythonConfiguration.toMap());
		Map<String, Object> combinedConfig = new HashMap<>(merge(env.getConfiguration(), create(pythonConfig)).asMap());
		env.setConfiguration(combinedConfig);
	}

	// --------------------------------------------------------------------------------------------

	public static void main(String[] args) {
		if (args.length < 1) {
			CliOptionsParser.printHelpClient();
			return;
		}

		switch (args[0]) {

			case MODE_EMBEDDED:
				// remove mode
				final String[] modeArgs = Arrays.copyOfRange(args, 1, args.length);
				final CliOptions options = CliOptionsParser.parseEmbeddedModeClient(modeArgs);
				if (options.isPrintHelp()) {
					CliOptionsParser.printHelpEmbeddedModeClient();
				} else {
						final SqlClient client = new SqlClient(true, options);
						client.start();
					
				}
				break;

			case MODE_GATEWAY:
				throw new SqlClientException("Gateway mode is not supported yet.");

			default:
				CliOptionsParser.printHelpClient();
		}
	}

	// --------------------------------------------------------------------------------------------

	private static class EmbeddedShutdownThread extends Thread {

		private final String sessionId;
		private final Executor executor;

		public EmbeddedShutdownThread(String sessionId, Executor executor) {
			this.sessionId = sessionId;
			this.executor = executor;
		}

		@Override
		public void run() {
			executor.closeSession(sessionId);
		}
	}
}
