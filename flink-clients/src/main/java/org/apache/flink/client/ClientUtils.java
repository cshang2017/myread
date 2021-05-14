

package org.apache.flink.client;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.client.program.ContextEnvironment;
import org.apache.flink.client.program.PackagedProgram;
import org.apache.flink.client.program.ProgramInvocationException;
import org.apache.flink.client.program.StreamContextEnvironment;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.core.execution.DetachedJobExecutionResult;
import org.apache.flink.core.execution.PipelineExecutorServiceLoader;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.runtime.execution.librarycache.FlinkUserCodeClassLoaders;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobmaster.JobResult;
import org.apache.flink.util.ExceptionUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URL;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.apache.flink.util.FlinkUserCodeClassLoader.NOOP_EXCEPTION_HANDLER;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Utility functions for Flink client.
 */
public enum ClientUtils {
	;

	public static ClassLoader buildUserCodeClassLoader(
			List<URL> jars,
			List<URL> classpaths,
			ClassLoader parent,
			Configuration configuration) {

		URL[] urls = new URL[jars.size() + classpaths.size()];
		for (int i = 0; i < jars.size(); i++) {
			urls[i] = jars.get(i);
		}
		for (int i = 0; i < classpaths.size(); i++) {
			urls[i + jars.size()] = classpaths.get(i);
		}
		final String[] alwaysParentFirstLoaderPatterns = CoreOptions.getParentFirstLoaderPatterns(configuration);
		final String classLoaderResolveOrder =
			configuration.getString(CoreOptions.CLASSLOADER_RESOLVE_ORDER);
		FlinkUserCodeClassLoaders.ResolveOrder resolveOrder =
			FlinkUserCodeClassLoaders.ResolveOrder.fromString(classLoaderResolveOrder);
		return FlinkUserCodeClassLoaders.create(resolveOrder, urls, parent, alwaysParentFirstLoaderPatterns, NOOP_EXCEPTION_HANDLER);
	}

	public static JobExecutionResult submitJob(ClusterClient<?> client, JobGraph jobGraph) {
	
		return client
			.submitJob(jobGraph)
			.thenApply(DetachedJobExecutionResult::new)
			.get();
	}

	public static JobExecutionResult submitJobAndWaitForResult(
			ClusterClient<?> client,
			JobGraph jobGraph,
			ClassLoader classLoader) {

		JobResult jobResult = client
				.submitJob(jobGraph)
				.thenCompose(client::requestJobResult)
				.get();
				try {
		return jobResult.toJobExecutionResult(classLoader);
	}

	public static void executeProgram(
			PipelineExecutorServiceLoader executorServiceLoader,
			Configuration configuration,
			PackagedProgram program,
			boolean enforceSingleJobExecution,
			boolean suppressSysout) {

		
		final ClassLoader userCodeClassLoader = program.getUserCodeClassLoader();
		final ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
		try {
			Thread.currentThread().setContextClassLoader(userCodeClassLoader);

			ContextEnvironment.setAsContext(
				executorServiceLoader,
				configuration,
				userCodeClassLoader,
				enforceSingleJobExecution,
				suppressSysout);

			StreamContextEnvironment.setAsContext(
				executorServiceLoader,
				configuration,
				userCodeClassLoader,
				enforceSingleJobExecution,
				suppressSysout);

			program.invokeInteractiveModeForExecution();
			ContextEnvironment.unsetAsContext();
			StreamContextEnvironment.unsetAsContext();
			
		} finally {
			Thread.currentThread().setContextClassLoader(contextClassLoader);
		}
	}
}
