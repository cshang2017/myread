package org.apache.flink.client.program;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.dag.Pipeline;
import org.apache.flink.client.FlinkPipelineTranslationUtil;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.optimizer.CompilerException;
import org.apache.flink.runtime.jobgraph.JobGraph;

import javax.annotation.Nullable;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.FileSystems;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * Utility class for {@link PackagedProgram} related operations.
 */
public enum PackagedProgramUtils {
	;

	private static final String PYTHON_DRIVER_CLASS_NAME = "org.apache.flink.client.python.PythonDriver";

	private static final String PYTHON_GATEWAY_CLASS_NAME = "org.apache.flink.client.python.PythonGatewayServer";

	/**
	 * Creates a {@link JobGraph} with a specified {@link JobID}
	 * from the given {@link PackagedProgram}.
	 *
	 * @param packagedProgram    to extract the JobGraph from
	 * @param configuration      to use for the optimizer and job graph generator
	 * @param defaultParallelism for the JobGraph
	 * @param jobID              the pre-generated job id
	 * @return JobGraph extracted from the PackagedProgram
	 * @throws ProgramInvocationException if the JobGraph generation failed
	 */
	public static JobGraph createJobGraph(
			PackagedProgram packagedProgram,
			Configuration configuration,
			int defaultParallelism,
			@Nullable JobID jobID,
			boolean suppressOutput) {
		final Pipeline pipeline = getPipelineFromProgram(packagedProgram, configuration,  defaultParallelism, suppressOutput);
		final JobGraph jobGraph = FlinkPipelineTranslationUtil.getJobGraphUnderUserClassLoader(
				packagedProgram.getUserCodeClassLoader(),
				pipeline,
				configuration,
				defaultParallelism);
		if (jobID != null) {
			jobGraph.setJobID(jobID);
		}
		jobGraph.addJars(packagedProgram.getJobJarAndDependencies());
		jobGraph.setClasspaths(packagedProgram.getClasspaths());
		jobGraph.setSavepointRestoreSettings(packagedProgram.getSavepointSettings());

		return jobGraph;
	}

	/**
	 * Creates a {@link JobGraph} with a random {@link JobID}
	 * from the given {@link PackagedProgram}.
	 *
	 * @param packagedProgram    to extract the JobGraph from
	 * @param configuration      to use for the optimizer and job graph generator
	 * @param defaultParallelism for the JobGraph
	 * @param suppressOutput     Whether to suppress stdout/stderr during interactive JobGraph creation.
	 * @return JobGraph extracted from the PackagedProgram
	 * @throws ProgramInvocationException if the JobGraph generation failed
	 */
	public static JobGraph createJobGraph(
			PackagedProgram packagedProgram,
			Configuration configuration,
			int defaultParallelism,
			boolean suppressOutput) throws ProgramInvocationException {
		return createJobGraph(packagedProgram, configuration, defaultParallelism, null, suppressOutput);
	}

	public static Pipeline getPipelineFromProgram(
			PackagedProgram program,
			Configuration configuration,
			int parallelism,
			boolean suppressOutput) {
		final ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();

		Thread.currentThread().setContextClassLoader(program.getUserCodeClassLoader());

		final PrintStream originalOut = System.out;
		final PrintStream originalErr = System.err;
		final ByteArrayOutputStream stdOutBuffer;
		final ByteArrayOutputStream stdErrBuffer;

		if (suppressOutput) {
			// temporarily write STDERR and STDOUT to a byte array.
			stdOutBuffer = new ByteArrayOutputStream();
			System.setOut(new PrintStream(stdOutBuffer));
			stdErrBuffer = new ByteArrayOutputStream();
			System.setErr(new PrintStream(stdErrBuffer));
		} else {
			stdOutBuffer = null;
			stdErrBuffer = null;
		}

		// temporary hack to support the optimizer plan preview
		OptimizerPlanEnvironment benv = new OptimizerPlanEnvironment(
			configuration,
			program.getUserCodeClassLoader(),
			parallelism);
		benv.setAsContext();
		StreamPlanEnvironment senv = new StreamPlanEnvironment(
			configuration,
			program.getUserCodeClassLoader(),
			parallelism);
		senv.setAsContext();

		try {
			program.invokeInteractiveModeForExecution();
		} finally {
			benv.unsetAsContext();
			senv.unsetAsContext();
			if (suppressOutput) {
				System.setOut(originalOut);
				System.setErr(originalErr);
			}
			Thread.currentThread().setContextClassLoader(contextClassLoader);
		}
	}

	public static Boolean isPython(String entryPointClassName) {
		return (entryPointClassName != null) &&
			(entryPointClassName.equals(PYTHON_DRIVER_CLASS_NAME) || entryPointClassName.equals(PYTHON_GATEWAY_CLASS_NAME));
	}

	public static URL getPythonJar() throws IOException {
		String flinkOptPath = System.getenv(ConfigConstants.ENV_FLINK_OPT_DIR);
		final List<Path> pythonJarPath = new ArrayList<>();
		Files.walkFileTree(
			FileSystems.getDefault().getPath(flinkOptPath), 
			new SimpleFileVisitor<Path>() {
				@Override
				public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
					FileVisitResult result = super.visitFile(file, attrs);
					if (file.getFileName().toString().startsWith("flink-python")) {
						pythonJarPath.add(file);
					}
					return result;
				}
			}
		);


		if (pythonJarPath.size() != 1) {
			throw new RuntimeException("Found " + pythonJarPath.size() + " flink-python jar.");
		}

		return pythonJarPath.get(0).toUri().toURL();
	}

	public static URI resolveURI(String path) throws URISyntaxException {
		final URI uri = new URI(path);
		if (uri.getScheme() != null) {
			return uri;
		}
		return new File(path).getAbsoluteFile().toURI();
	}
}
