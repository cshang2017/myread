package org.apache.flink.runtime.entrypoint.parser;

import org.apache.commons.cli.Option;

/**
 * Container class for command line options.
 */
public class CommandLineOptions {

	public static final Option CONFIG_DIR_OPTION = Option.builder("c")
		.longOpt("configDir")
		.required(true)
		.hasArg(true)
		.argName("configuration directory")
		.desc("Directory which contains the configuration file flink-conf.yml.")
		.build();

	public static final Option REST_PORT_OPTION = Option.builder("r")
		.longOpt("webui-port")
		.required(false)
		.hasArg(true)
		.argName("rest port")
		.desc("Port for the rest endpoint and the web UI.")
		.build();

	public static final Option DYNAMIC_PROPERTY_OPTION = Option.builder("D")
		.argName("property=value")
		.numberOfArgs(2)
		.valueSeparator('=')
		.desc("use value for given property")
		.build();

	public static final Option HOST_OPTION = Option.builder("h")
		.longOpt("host")
		.required(false)
		.hasArg(true)
		.argName("hostname")
		.desc("Hostname for the RPC service.")
		.build();

	/**
	 * @deprecated exists only for compatibility with legacy mode. Remove once legacy mode
	 * and execution mode option has been removed.
	 */
	@Deprecated
	public static final Option EXECUTION_MODE_OPTION = Option.builder("x")
		.longOpt("executionMode")
		.required(false)
		.hasArg(true)
		.argName("execution mode")
		.desc("Deprecated option")
		.build();

	private CommandLineOptions() {}
}
