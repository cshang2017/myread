package org.apache.flink.client.python;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.functions.python.PythonFunction;

import py4j.GatewayServer;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.flink.client.python.PythonEnvUtils.getGatewayServer;
import static org.apache.flink.client.python.PythonEnvUtils.launchPy4jPythonClient;
import static org.apache.flink.client.python.PythonEnvUtils.setGatewayServer;
import static org.apache.flink.client.python.PythonEnvUtils.startGatewayServer;

/**
 * The factory which creates the PythonFunction objects from given module name and object name.
 */
public interface PythonFunctionFactory {

	long CHECK_INTERVAL = 100;
	long TIMEOUT_MILLIS = 10000;

	AtomicReference<PythonFunctionFactory> PYTHON_FUNCTION_FACTORY_REF = new AtomicReference<>();

	/**
	 * Returns PythonFunction according to moduleName and objectName.
	 *
	 * @param moduleName The module name of the Python UDF.
	 * @param objectName The function name / class name of the Python UDF.
	 * @return The PythonFunction object which represents the Python UDF.
	 */
	PythonFunction getPythonFunction(String moduleName, String objectName);

	/**
	 * Returns PythonFunction according to the fully qualified name of the Python UDF
	 * i.e ${moduleName}.${functionName} or ${moduleName}.${className}.
	 *
	 * @param fullyQualifiedName The fully qualified name of the Python UDF.
	 * @param config The configuration of python dependencies.
	 * @return The PythonFunction object which represents the Python UDF.
	 */
	static PythonFunction getPythonFunction(String fullyQualifiedName, ReadableConfig config)
	{
		int splitIndex = fullyQualifiedName.lastIndexOf(".");
		
		String moduleName = fullyQualifiedName.substring(0, splitIndex);
		String objectName = fullyQualifiedName.substring(splitIndex + 1);

		Configuration mergedConfig =
			new Configuration(ExecutionEnvironment.getExecutionEnvironment().getConfiguration());
		mergedConfig.addAll((Configuration) config);
		PythonFunctionFactory pythonFunctionFactory = getPythonFunctionFactory(mergedConfig);
		return pythonFunctionFactory.getPythonFunction(moduleName, objectName);
	}

	static PythonFunctionFactory getPythonFunctionFactory(ReadableConfig config)
			 {
		synchronized (PythonFunctionFactory.class) {
			if (PYTHON_FUNCTION_FACTORY_REF.get() != null) {
				return PYTHON_FUNCTION_FACTORY_REF.get();
			} else {
				Map<String, Object> entryPoint;
				if (getGatewayServer() == null) {
						GatewayServer gatewayServer = startGatewayServer();
						setGatewayServer(gatewayServer);
						List<String> commands = new ArrayList<>();
						commands.add("-m");
						commands.add("pyflink.pyflink_callback_server");
						String tmpDir = System.getProperty("java.io.tmpdir") +
						File.separator + "pyflink" + File.separator + UUID.randomUUID();
						Process pythonProcess = launchPy4jPythonClient(
							gatewayServer, config, commands, null, tmpDir, false);
						entryPoint = (Map<String, Object>) gatewayServer.getGateway().getEntryPoint();
						int i = 0;
						while (!entryPoint.containsKey("PythonFunctionFactory")) {
							assert(pythonProcess.isAlive());
							
							Thread.sleep(CHECK_INTERVAL);
							
							i++;
							if (i > TIMEOUT_MILLIS / CHECK_INTERVAL) {
								throw new RuntimeException("Python callback server start failed!");
							}
						}
				
					Runtime.getRuntime().addShutdownHook(new PythonProcessShutdownHook(pythonProcess));
				} else {
					entryPoint = (Map<String, Object>) getGatewayServer().getGateway().getEntryPoint();
				}
				PythonFunctionFactory pythonFunctionFactory =
					(PythonFunctionFactory) entryPoint.get("PythonFunctionFactory");
				PYTHON_FUNCTION_FACTORY_REF.set(pythonFunctionFactory);
				return pythonFunctionFactory;
			}
		}
	}

	static void shutdownPythonProcess(Process pythonProcess, long timeoutMillis) {
		pythonProcess.destroy();
		pythonProcess.waitFor(timeoutMillis, TimeUnit.MILLISECONDS);
		
		if (pythonProcess.isAlive()) {
			pythonProcess.destroyForcibly();
		}
	}


	class PythonProcessShutdownHook extends Thread {

		private Process process;

		public PythonProcessShutdownHook(Process process) {
			this.process = process;
		}

		@Override
		public void run() {
			shutdownPythonProcess(process, TIMEOUT_MILLIS);
		}
	}
}
