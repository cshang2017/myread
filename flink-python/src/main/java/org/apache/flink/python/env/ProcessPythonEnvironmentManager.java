package org.apache.flink.python.env;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.python.util.PythonEnvironmentManagerUtils;
import org.apache.flink.python.util.ZipUtils;
import org.apache.flink.util.FileUtils;
import org.apache.flink.util.ShutdownHookUtil;

import org.apache.flink.shaded.guava18.com.google.common.base.Strings;

import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.core.construction.Environments;
import org.codehaus.commons.nullanalysis.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.UUID;

/**
 * The ProcessPythonEnvironmentManager is used to prepare the working dir of python UDF worker and create
 * ProcessEnvironment object of Beam Fn API. It's used when the python function runner is configured to run python UDF
 * in process mode.
 */
@Internal
public final class ProcessPythonEnvironmentManager implements PythonEnvironmentManager {

	static final String PYFLINK_GATEWAY_DISABLED = "PYFLINK_GATEWAY_DISABLED";
	public static final String PYTHON_REQUIREMENTS_FILE = "_PYTHON_REQUIREMENTS_FILE";
	public static final String PYTHON_REQUIREMENTS_CACHE = "_PYTHON_REQUIREMENTS_CACHE";
	public static final String PYTHON_REQUIREMENTS_INSTALL_DIR = "_PYTHON_REQUIREMENTS_INSTALL_DIR";
	public static final String PYTHON_WORKING_DIR = "_PYTHON_WORKING_DIR";

	static final String PYTHON_REQUIREMENTS_DIR = "python-requirements";
	static final String PYTHON_ARCHIVES_DIR = "python-archives";
	static final String PYTHON_FILES_DIR = "python-files";

	private static final long CHECK_INTERVAL = 20;
	private static final long CHECK_TIMEOUT = 1000;

	private transient String baseDirectory;

	/**
	 * Directory for storing the installation result of the requirements file.
	 */
	private transient String requirementsDirectory;

	/**
	 * Directory for storing the extracted result of the archive files.
	 */
	private transient String archivesDirectory;

	/**
	 * Directory for storing the uploaded python files.
	 */
	private transient String filesDirectory;

	private transient Thread shutdownHook;

	@NotNull private final PythonDependencyInfo dependencyInfo;
	@NotNull private final Map<String, String> systemEnv;
	@NotNull private final String[] tmpDirectories;

	public ProcessPythonEnvironmentManager(
		@NotNull PythonDependencyInfo dependencyInfo,
		@NotNull String[] tmpDirectories,
		@NotNull Map<String, String> systemEnv) {
		this.dependencyInfo = Objects.requireNonNull(dependencyInfo);
		this.tmpDirectories = Objects.requireNonNull(tmpDirectories);
		this.systemEnv = Objects.requireNonNull(systemEnv);
	}

	@Override
	public void open() {
		baseDirectory = createBaseDirectory(tmpDirectories);
		archivesDirectory = String.join(File.separator, baseDirectory, PYTHON_ARCHIVES_DIR);
		requirementsDirectory = String.join(File.separator, baseDirectory, PYTHON_REQUIREMENTS_DIR);
		filesDirectory = String.join(File.separator, baseDirectory, PYTHON_FILES_DIR);

		File baseDirectoryFile = new File(baseDirectory);
		shutdownHook = ShutdownHookUtil.addShutdownHook(
			this, ProcessPythonEnvironmentManager.class.getSimpleName(), null);
	}

	@Override
	public void close() throws Exception {
		try {
			int retries = 0;
			while (true) {
					FileUtils.deleteDirectory(new File(baseDirectory));
					break;
			}
		} finally {
			if (shutdownHook != null) {
				ShutdownHookUtil.removeShutdownHook(
					shutdownHook, ProcessPythonEnvironmentManager.class.getSimpleName(), LOG);
				shutdownHook = null;
			}
		}
	}

	@Override
	public RunnerApi.Environment createEnvironment() {

		Map<String, String> env = constructEnvironmentVariables();

		if (dependencyInfo.getRequirementsFilePath().isPresent()) {
			PythonEnvironmentManagerUtils.pipInstallRequirements(
				dependencyInfo.getRequirementsFilePath().get(),
				dependencyInfo.getRequirementsCacheDir().orElse(null),
				requirementsDirectory,
				dependencyInfo.getPythonExec(),
				env);
		}
		String runnerScript = PythonEnvironmentManagerUtils.getPythonUdfRunnerScript(dependencyInfo.getPythonExec(), env);

		return Environments.createProcessEnvironment(
			"",
			"",
			runnerScript,
			env);
	}

	/**
	 * Returns an empty RetrievalToken because no files will be transmit via ArtifactService in process mode.
	 *
	 * @return The path of empty RetrievalToken.
	 */
	@Override
	public String createRetrievalToken() {
		File retrievalToken = new File(baseDirectory,
			"retrieval_token_" + UUID.randomUUID().toString() + ".json");
		if (retrievalToken.createNewFile()) {
			final DataOutputStream dos = new DataOutputStream(new FileOutputStream(retrievalToken));
			dos.writeBytes("{\"manifest\": {}}");
			dos.flush();
			dos.close();
			return retrievalToken.getAbsolutePath();
		} 
	}

	/**
	 * Constructs the environment variables which is used to launch the python UDF worker.
	 *
	 * <p>To avoid unnecessary IO, the artifacts will not be transmitted via the ArtifactService of Beam when running in
	 * process mode. Instead, the paths of the artifacts will be passed to the Python UDF worker directly.
	 *
	 * @return The environment variables which contain the paths of the python dependencies.
	 */
	@VisibleForTesting
	Map<String, String> constructEnvironmentVariables() {
		Map<String, String> env = new HashMap<>(this.systemEnv);

		constructFilesDirectory(env);

		constructArchivesDirectory(env);

		constructRequirementsDirectory(env);

		// set BOOT_LOG_DIR.
		env.put("BOOT_LOG_DIR", baseDirectory);

		// disable the launching of gateway server to prevent from this dead loop:
		// launch UDF worker -> import udf -> import job code
		//        ^                                    | (If the job code is not enclosed in a
		//        |                                    |  if name == 'main' statement)
		//        |                                    V
		// execute job in local mode <- launch gateway server and submit job to local executor
		env.put(PYFLINK_GATEWAY_DISABLED, "true");

		// set the path of python interpreter, it will be used to execute the udf worker.
		env.put("python", dependencyInfo.getPythonExec());
		return env;
	}

	private void constructFilesDirectory(Map<String, String> env) {
		// link or copy python files to filesDirectory and add them to PYTHONPATH
		List<String> pythonFilePaths = new ArrayList<>();
		for (Map.Entry<String, String> entry : dependencyInfo.getPythonFiles().entrySet()) {
			// The origin file name will be wiped when downloaded from the distributed cache, restore the origin name to
			// make sure the python files could be imported.
			// The path of the restored python file will be as following:
			// ${baseDirectory}/${PYTHON_FILES_DIR}/${distributedCacheFileName}/${originFileName}
			String distributedCacheFileName = new File(entry.getKey()).getName();
			String originFileName = entry.getValue();

			Path target = FileSystems.getDefault().getPath(filesDirectory, distributedCacheFileName, originFileName);
			target.getParent().toFile().mkdirs();

			Path src = FileSystems.getDefault().getPath(entry.getKey());
			Files.createSymbolicLink(target, src);
			

			File pythonFile = new File(entry.getKey());
			String pythonPath;
			if (pythonFile.isFile() && originFileName.endsWith(".py")) {
				// If the python file is file with suffix .py, add the parent directory to PYTHONPATH.
				pythonPath = String.join(File.separator, filesDirectory, distributedCacheFileName);
			} else {
				pythonPath = String.join(File.separator, filesDirectory, distributedCacheFileName, originFileName);
			}
			pythonFilePaths.add(pythonPath);
		}
		appendToPythonPath(env, pythonFilePaths);
	}

	private void constructArchivesDirectory(Map<String, String> env) throws IOException {
		if (!dependencyInfo.getArchives().isEmpty()) {
			// set the archives directory as the working directory, then user could access the content of the archives
			// via relative path
			env.put(PYTHON_WORKING_DIR, archivesDirectory);

			// extract archives to archives directory
			for (Map.Entry<String, String> entry : dependencyInfo.getArchives().entrySet()) {
				ZipUtils.extractZipFileWithPermissions(
					entry.getKey(), String.join(File.separator, archivesDirectory, entry.getValue()));
			}
		}
	}

	private void constructRequirementsDirectory(Map<String, String> env) throws IOException {
		// set the requirements file and the dependencies specified by the requirements file will be installed in
		// boot.py during initialization
		if (dependencyInfo.getRequirementsFilePath().isPresent()) {
			File requirementsDirectoryFile = new File(requirementsDirectory);
			requirementsDirectoryFile.mkdirs();

			env.put(PYTHON_REQUIREMENTS_FILE, dependencyInfo.getRequirementsFilePath().get());

			if (dependencyInfo.getRequirementsCacheDir().isPresent()) {
				env.put(PYTHON_REQUIREMENTS_CACHE, dependencyInfo.getRequirementsCacheDir().get());
			}

			// the dependencies specified by the requirements file will be installed into this directory, and will be
			// added to PYTHONPATH in boot.py
			env.put(PYTHON_REQUIREMENTS_INSTALL_DIR, requirementsDirectory);
		}
	}

	@VisibleForTesting
	String getBaseDirectory() {
		return baseDirectory;
	}

	@Override
	public String getBootLog()  {
		File bootLogFile = new File(baseDirectory + File.separator + "flink-python-udf-boot.log");
		String msg = "Failed to create stage bundle factory!";
		if (bootLogFile.exists()) {
			byte[] output = Files.readAllBytes(bootLogFile.toPath());
			msg += String.format(" %s", new String(output, Charset.defaultCharset()));
		}
		return msg;
	}

	private static void appendToPythonPath(Map<String, String> env, List<String> pythonDependencies) {
		if (pythonDependencies.isEmpty()) {
			return;
		}

		String pythonDependencyPath = String.join(File.pathSeparator, pythonDependencies);
		String pythonPath = env.get("PYTHONPATH");
		if (Strings.isNullOrEmpty(pythonPath)) {
			env.put("PYTHONPATH", pythonDependencyPath);
		} else {
			env.put("PYTHONPATH", String.join(File.pathSeparator, pythonDependencyPath, pythonPath));
		}
	}

	private static String createBaseDirectory(String[] tmpDirectories)  {
		Random rnd = new Random();
		// try to find a unique file name for the base directory
		int maxAttempts = 10;
		for (int attempt = 0; attempt < maxAttempts; attempt++) {
			String directory = tmpDirectories[rnd.nextInt(tmpDirectories.length)];
			File baseDirectory = new File(directory, "python-dist-" + UUID.randomUUID().toString());
			if (baseDirectory.mkdirs()) {
				return baseDirectory.getAbsolutePath();
			}
		}
	}
}
