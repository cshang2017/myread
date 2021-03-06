
package org.apache.flink.client.python;

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.core.fs.Path;
import org.apache.flink.util.FileUtils;
import org.apache.flink.util.NetUtils;
import org.apache.flink.util.OperatingSystem;

import org.apache.flink.shaded.guava18.com.google.common.base.Strings;

import py4j.CallbackClient;
import py4j.Gateway;
import py4j.GatewayServer;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.file.FileSystems;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;

import static org.apache.flink.python.PythonOptions.PYTHON_CLIENT_EXECUTABLE;
import static org.apache.flink.python.PythonOptions.PYTHON_FILES;
import static org.apache.flink.python.util.PythonDependencyUtils.FILE_DELIMITER;

/**
 * The util class help to prepare Python env and run the python process.
 */
final class PythonEnvUtils {

	static final String PYFLINK_CLIENT_EXECUTABLE = "PYFLINK_CLIENT_EXECUTABLE";

	/**
	 * Wraps Python exec environment.
	 */
	static class   PythonEnvironment {
		String tempDirectory;
		String pythonExec = OperatingSystem.isWindows() ? "python.exe" : "python";
		String pythonPath;

		Map<String, String> systemEnv = new HashMap<>();
	}

	/**
	 * The hook thread that delete the tmp working dir of python process after the python process shutdown.
	 */
	private static class ShutDownPythonHook extends Thread {
		private Process p;
		private String pyFileDir;

		ShutDownPythonHook(Process p, String pyFileDir) {
			this.p = p;
			this.pyFileDir = pyFileDir;
		}

		public void run() {

			p.destroyForcibly();

			if (pyFileDir != null) {
				File pyDir = new File(pyFileDir);
				FileUtils.deleteDirectoryQuietly(pyDir);
			}
		}
	}

	/**
	 * Prepares PythonEnvironment to start python process.
	 *
	 * @param config The Python configurations.
	 * @param entryPointScript The entry point script, optional.
	 * @param tmpDir The temporary directory which files will be copied to.
	 * @return PythonEnvironment the Python environment which will be executed in Python process.
	 */
	static PythonEnvironment preparePythonEnvironment(
		ReadableConfig config,
		String entryPointScript,
		String tmpDir) {

		PythonEnvironment env = new PythonEnvironment();

		// 1. set the path of python interpreter.
		String pythonExec = config.getOptional(PYTHON_CLIENT_EXECUTABLE)
			.orElse(System.getenv(PYFLINK_CLIENT_EXECUTABLE));
		if (pythonExec != null) {
			env.pythonExec = pythonExec;
		}

		// 2. setup temporary local directory for the user files
		tmpDir = new File(tmpDir).getAbsolutePath();
		Path tmpDirPath = new Path(tmpDir);
		tmpDirPath.getFileSystem().mkdirs(tmpDirPath);
		env.tempDirectory = tmpDir;

		// 3. append the internal lib files to PYTHONPATH.
		if (System.getenv(ConfigConstants.ENV_FLINK_OPT_DIR) != null) {
			String pythonLibDir = System.getenv(ConfigConstants.ENV_FLINK_OPT_DIR) + File.separator + "python";
			env.pythonPath = getLibFiles(pythonLibDir).stream()
				.map(p -> p.toFile().getAbsolutePath())
				.collect(Collectors.joining(File.pathSeparator));
		}

		// 4. copy relevant python files to tmp dir and set them in PYTHONPATH.
		if (config.getOptional(PYTHON_FILES).isPresent()) {
			List<Path> pythonFiles = Arrays.stream(config.get(PYTHON_FILES).split(FILE_DELIMITER))
				.map(Path::new).collect(Collectors.toList());
			addToPythonPath(env, pythonFiles);
		}
		if (entryPointScript != null) {
			addToPythonPath(env, Collections.singletonList(new Path(entryPointScript)));
		}
		return env;
	}

	/**
	 * Creates symbolLink in working directory for pyflink lib.
	 *
	 * @param libPath          the pyflink lib file path.
	 * @param symbolicLinkPath the symbolic link to pyflink lib.
	 */
	private static void createSymbolicLink(java.nio.file.Path libPath, java.nio.file.Path symbolicLinkPath)
			throws IOException {
			Files.createSymbolicLink(symbolicLinkPath, libPath);
	}

	/**
	 * Gets pyflink dependent libs in specified directory.
	 *
	 * @param libDir The lib directory
	 */
	private static List<java.nio.file.Path> getLibFiles(String libDir) {
		final List<java.nio.file.Path> libFiles = new ArrayList<>();

		SimpleFileVisitor<java.nio.file.Path> finder = new SimpleFileVisitor<java.nio.file.Path>() {
			@Override
			public FileVisitResult visitFile(java.nio.file.Path file, BasicFileAttributes attrs) {
				// only include zip file
				if (file.toString().endsWith(".zip")) {
					libFiles.add(file);
				}
				return FileVisitResult.CONTINUE;
			}
		};
		
		Files.walkFileTree(FileSystems.getDefault().getPath(libDir), finder);

		return libFiles;
	}

	private static void addToPythonPath(PythonEnvironment env, List<Path> pythonFiles) throws IOException {
		List<String> pythonPathList = new ArrayList<>();
		Path tmpDirPath = new Path(env.tempDirectory);

		for (Path pythonFile : pythonFiles) {
			String sourceFileName = pythonFile.getName();
			// add random UUID parent directory to avoid name conflict.
			Path targetPath = new Path(
				tmpDirPath,
				String.join(File.separator, UUID.randomUUID().toString(), sourceFileName));
			if (!pythonFile.getFileSystem().isDistributedFS()) {
				// if the path is local file, try to create symbolic link.
				new File(targetPath.getParent().toString()).mkdir();
				createSymbolicLink(
					Paths.get(new File(pythonFile.getPath()).getAbsolutePath()),
					Paths.get(targetPath.toString()));
			} else {
				FileUtils.copy(pythonFile, targetPath, true);
			}
			if (Files.isRegularFile(Paths.get(targetPath.toString()).toRealPath()) && sourceFileName.endsWith(".py")) {
				// add the parent directory of .py file itself to PYTHONPATH
				pythonPathList.add(targetPath.getParent().toString());
			} else {
				pythonPathList.add(targetPath.toString());
			}
		}

		if (env.pythonPath != null && !env.pythonPath.isEmpty()) {
			pythonPathList.add(env.pythonPath);
		}
		env.pythonPath = String.join(File.pathSeparator, pythonPathList);
	}

	/**
	 * Starts python process.
	 *
	 * @param pythonEnv the python Environment which will be in a process.
	 * @param commands  the commands that python process will execute.
	 * @return the process represent the python process.
	 * @throws IOException Thrown if an error occurred when python process start.
	 */
	static Process startPythonProcess(
		PythonEnvironment pythonEnv,
		List<String> commands,
		boolean redirectToPipe) {

		ProcessBuilder pythonProcessBuilder = new ProcessBuilder();
		Map<String, String> env = pythonProcessBuilder.environment();

		if (pythonEnv.pythonPath != null) {
			String defaultPythonPath = env.get("PYTHONPATH");
			if (Strings.isNullOrEmpty(defaultPythonPath)) {
				env.put("PYTHONPATH", pythonEnv.pythonPath);
			} else {
				env.put("PYTHONPATH", String.join(File.pathSeparator, pythonEnv.pythonPath, defaultPythonPath));
			}
		}
		pythonEnv.systemEnv.forEach(env::put);
		commands.add(0, pythonEnv.pythonExec);
		pythonProcessBuilder.command(commands);
		// redirect the stderr to stdout
		pythonProcessBuilder.redirectErrorStream(true);
		if (redirectToPipe) {
			pythonProcessBuilder.redirectOutput(ProcessBuilder.Redirect.PIPE);
		} else {
			// set the child process the output same as the parent process.
			pythonProcessBuilder.redirectOutput(ProcessBuilder.Redirect.INHERIT);
		}
		Process process = pythonProcessBuilder.start();
		assert(process.isAlive());

		// Make sure that the python sub process will be killed when JVM exit
		ShutDownPythonHook hook = new ShutDownPythonHook(process, pythonEnv.tempDirectory);
		Runtime.getRuntime().addShutdownHook(hook);

		return process;
	}

	/**
	 * Creates a GatewayServer run in a daemon thread.
	 *
	 * @return The created GatewayServer
	 */
	static GatewayServer startGatewayServer()  {
		CompletableFuture<GatewayServer> gatewayServerFuture = new CompletableFuture<>();
		Thread thread = new Thread(() -> {
			int freePort = NetUtils.getAvailablePort();
			GatewayServer server = new GatewayServer.GatewayServerBuilder()
					.gateway(new Gateway(new ConcurrentHashMap<String, Object>(), new CallbackClient(freePort)))
					.javaPort(0)
					.build();
			resetCallbackClientExecutorService(server);
			gatewayServerFuture.complete(server);
			server.start(true);
		});
		thread.setName("py4j-gateway");
		thread.setDaemon(true);
		thread.start();
		thread.join();
		return gatewayServerFuture.get();
	}

	/**
	 * Reset a daemon thread to the callback client thread pool so that the callback server can be terminated when gate
	 * way server is shutting down. We need to shut down the none-daemon thread firstly, then set a new thread created
	 * in a daemon thread to the ExecutorService.
	 *
	 * @param gatewayServer the gateway which creates the callback server.
	 * */
	private static void resetCallbackClientExecutorService(GatewayServer gatewayServer)  {
		CallbackClient callbackClient = (CallbackClient) gatewayServer.getCallbackClient();
		// The Java API of py4j does not provide approach to set "daemonize_connections" parameter.
		// Use reflect to daemonize the connection thread.
		Field executor = CallbackClient.class.getDeclaredField("executor");
		executor.setAccessible(true);
		((ScheduledExecutorService) executor.get(callbackClient)).shutdown();
		executor.set(callbackClient, Executors.newScheduledThreadPool(1, Thread::new));
		Method setupCleaner = CallbackClient.class.getDeclaredMethod("setupCleaner");
		setupCleaner.setAccessible(true);
		setupCleaner.invoke(callbackClient);
	}

	/**
	 * Reset the callback client of gatewayServer with the given callbackListeningAddress and callbackListeningPort
	 * after the callback server started.
	 *
	 * @param callbackServerListeningAddress the listening address of the callback server.
	 * @param callbackServerListeningPort the listening port of the callback server.
	 * */
	public static void resetCallbackClient(String callbackServerListeningAddress, int callbackServerListeningPort)  {

		gatewayServer = getGatewayServer();
		gatewayServer.resetCallbackClient(InetAddress.getByName(callbackServerListeningAddress), callbackServerListeningPort);
		resetCallbackClientExecutorService(gatewayServer);
	}

	/**
	 * Py4J both supports Java to Python RPC and Python to Java RPC. The GatewayServer object is
	 * the entry point of Java to Python RPC. Since the Py4j Python client will only be launched
	 * only once, the GatewayServer object needs to be reused.
	 */
	private static GatewayServer gatewayServer = null;

	static GatewayServer getGatewayServer() {
		return gatewayServer;
	}

	static void setGatewayServer(GatewayServer gatewayServer) {
		PythonEnvUtils.gatewayServer = gatewayServer;
	}

	static Process launchPy4jPythonClient(
		GatewayServer gatewayServer,
		ReadableConfig config,
		List<String> commands,
		String entryPointScript,
		String tmpDir,
		boolean redirectToPipe) throws IOException {

		PythonEnvironment pythonEnv = PythonEnvUtils.preparePythonEnvironment(
			config, entryPointScript, tmpDir);
		// set env variable PYFLINK_GATEWAY_PORT for connecting of python gateway in python process.
		pythonEnv.systemEnv.put("PYFLINK_GATEWAY_PORT", String.valueOf(gatewayServer.getListeningPort()));
		// start the python process.
		return PythonEnvUtils.startPythonProcess(pythonEnv, commands, redirectToPipe);
	}
}
