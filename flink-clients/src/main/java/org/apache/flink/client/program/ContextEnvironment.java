package org.apache.flink.client.program;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.ExecutionEnvironmentFactory;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.core.execution.DetachedJobExecutionResult;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.JobListener;
import org.apache.flink.core.execution.PipelineExecutorServiceLoader;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.ShutdownHookUtil;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Execution Environment for remote execution with the Client.
 */
public class ContextEnvironment extends ExecutionEnvironment {

	private final boolean suppressSysout;
	private final boolean enforceSingleJobExecution;

	private int jobCounter;

	public ContextEnvironment(
			final PipelineExecutorServiceLoader executorServiceLoader,
			final Configuration configuration,
			final ClassLoader userCodeClassLoader,
			final boolean enforceSingleJobExecution,
			final boolean suppressSysout) {
		super(executorServiceLoader, configuration, userCodeClassLoader);
		this.suppressSysout = suppressSysout;
		this.enforceSingleJobExecution = enforceSingleJobExecution;

		this.jobCounter = 0;
	}

	@Override
	public JobExecutionResult execute(String jobName)  {
		final JobClient jobClient = executeAsync(jobName);
		final List<JobListener> jobListeners = getJobListeners();
		
		final JobExecutionResult  jobExecutionResult = getJobExecutionResult(jobClient);
		jobListeners.forEach(jobListener ->
				jobListener.onJobExecuted(jobExecutionResult, null));
		return jobExecutionResult;
	}

	private JobExecutionResult getJobExecutionResult(final JobClient jobClient) {

		JobExecutionResult jobExecutionResult;
		if (getConfiguration().getBoolean(DeploymentOptions.ATTACHED)) {
			CompletableFuture<JobExecutionResult> jobExecutionResultFuture =
					jobClient.getJobExecutionResult(getUserCodeClassLoader());

			if (getConfiguration().getBoolean(DeploymentOptions.SHUTDOWN_IF_ATTACHED)) {
				Thread shutdownHook = ShutdownHookUtil.addShutdownHook(
						() -> {
							// wait a smidgen to allow the async request to go through before
							// the jvm exits
							jobClient.cancel().get(1, TimeUnit.SECONDS);
						},
						ContextEnvironment.class.getSimpleName());
				jobExecutionResultFuture.whenComplete((ignored, throwable) ->
						ShutdownHookUtil.removeShutdownHook(
							shutdownHook, ContextEnvironment.class.getSimpleName()));
			}

			jobExecutionResult = jobExecutionResultFuture.get();
		} else {
			jobExecutionResult = new DetachedJobExecutionResult(jobClient.getJobID());
		}

		return jobExecutionResult;
	}

	@Override
	public JobClient executeAsync(String jobName) throws Exception {
		validateAllowedExecution();
		final JobClient jobClient = super.executeAsync(jobName);

		return jobClient;
	}

	private void validateAllowedExecution() {
		if (enforceSingleJobExecution && jobCounter > 0) {
			throw new FlinkRuntimeException("Cannot have more than one execute() or executeAsync() call in a single environment.");
		}
		jobCounter++;
	}

	@Override
	public String toString() {
		return "Context Environment (parallelism = " + (getParallelism() == ExecutionConfig.PARALLELISM_DEFAULT ? "default" : getParallelism()) + ")";
	}

	// --------------------------------------------------------------------------------------------

	public static void setAsContext(
			final PipelineExecutorServiceLoader executorServiceLoader,
			final Configuration configuration,
			final ClassLoader userCodeClassLoader,
			final boolean enforceSingleJobExecution,
			final boolean suppressSysout) {
		ExecutionEnvironmentFactory factory = () -> new ContextEnvironment(
			executorServiceLoader,
			configuration,
			userCodeClassLoader,
			enforceSingleJobExecution,
			suppressSysout);
		initializeContextEnvironment(factory);
	}

	public static void unsetAsContext() {
		resetContextEnvironment();
	}
}
