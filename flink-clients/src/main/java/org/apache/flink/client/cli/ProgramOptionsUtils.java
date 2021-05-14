package org.apache.flink.client.cli;

import org.apache.flink.client.program.PackagedProgramUtils;

import org.apache.commons.cli.CommandLine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.URL;
import java.net.URLClassLoader;

import static org.apache.flink.client.cli.CliFrontendParser.CLASS_OPTION;
import static org.apache.flink.client.cli.CliFrontendParser.PYARCHIVE_OPTION;
import static org.apache.flink.client.cli.CliFrontendParser.PYEXEC_OPTION;
import static org.apache.flink.client.cli.CliFrontendParser.PYFILES_OPTION;
import static org.apache.flink.client.cli.CliFrontendParser.PYMODULE_OPTION;
import static org.apache.flink.client.cli.CliFrontendParser.PYREQUIREMENTS_OPTION;
import static org.apache.flink.client.cli.CliFrontendParser.PY_OPTION;

/**
 * Utility class for {@link ProgramOptions}.
 */
public enum ProgramOptionsUtils {
	;


	/**
	 * @return True if the commandline contains "-py" or "-pym" options or comes from PyFlink shell, false otherwise.
	 */
	public static boolean isPythonEntryPoint(CommandLine line) {
		return line.hasOption(PY_OPTION.getOpt()) ||
			line.hasOption(PYMODULE_OPTION.getOpt()) ||
			"org.apache.flink.client.python.PythonGatewayServer".equals(line.getOptionValue(CLASS_OPTION.getOpt()));
	}

	/**
	 * @return True if the commandline contains "-pyfs", "-pyarch", "-pyreq", "-pyexec" options, false otherwise.
	 */
	public static boolean containsPythonDependencyOptions(CommandLine line) {
		return line.hasOption(PYFILES_OPTION.getOpt()) ||
			line.hasOption(PYREQUIREMENTS_OPTION.getOpt()) ||
			line.hasOption(PYARCHIVE_OPTION.getOpt()) ||
			line.hasOption(PYEXEC_OPTION.getOpt());
	}

	public static ProgramOptions createPythonProgramOptions(CommandLine line) throws CliArgsException {
			ClassLoader classLoader = new URLClassLoader(
					new URL[]{PackagedProgramUtils.getPythonJar()},
					Thread.currentThread().getContextClassLoader());
;
			Class<?> pythonProgramOptionsClazz = Class.forName(
				"org.apache.flink.client.cli.PythonProgramOptions",
				false,
				classLoader);
			Constructor<?> constructor = pythonProgramOptionsClazz.getConstructor(CommandLine.class);
			return (ProgramOptions) constructor.newInstance(line);
	
	}
}
