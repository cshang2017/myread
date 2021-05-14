package org.apache.flink.runtime.jobmaster;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.execution.librarycache.LibraryCacheManager;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.highavailability.RunningJobsRegistry;
import org.apache.flink.runtime.highavailability.RunningJobsRegistry.JobSchedulingStatus;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobmanager.OnCompletionActions;
import org.apache.flink.runtime.jobmaster.factories.JobMasterServiceFactory;
import org.apache.flink.runtime.leaderelection.LeaderContender;
import org.apache.flink.runtime.leaderelection.LeaderElectionService;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.function.FunctionUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The runner for the job manager. It deals with job level leader election and make underlying job manager
 * properly reacted.
 */
public class JobManagerRunnerImpl implements LeaderContender, OnCompletionActions, JobManagerRunner {

	/** Lock to ensure that this runner can deal with leader election event and job completion notifies simultaneously. */
	private final Object lock = new Object();

	/** The job graph needs to run. */
	private final JobGraph jobGraph;

	/** Used to check whether a job needs to be run. */
	private final RunningJobsRegistry runningJobsRegistry;

	/** Leader election for this job. */
	private final LeaderElectionService leaderElectionService;

	private final LibraryCacheManager.ClassLoaderLease classLoaderLease;

	private final Executor executor;

	private final JobMasterService jobMasterService;

	private final FatalErrorHandler fatalErrorHandler;

	private final CompletableFuture<ArchivedExecutionGraph> resultFuture;

	private final CompletableFuture<Void> terminationFuture;

	private CompletableFuture<Void> leadershipOperation;

	/** flag marking the runner as shut down. */
	private volatile boolean shutdown;

	private volatile CompletableFuture<JobMasterGateway> leaderGatewayFuture;

	// ------------------------------------------------------------------------

	/**
	 * Exceptions that occur while creating the JobManager or JobManagerRunnerImpl are directly
	 * thrown and not reported to the given {@code FatalErrorHandler}.
	 *
	 * @throws Exception Thrown if the runner cannot be set up, because either one of the
	 *                   required services could not be started, or the Job could not be initialized.
	 */
	public JobManagerRunnerImpl(
			final JobGraph jobGraph,
			final JobMasterServiceFactory jobMasterFactory,
			final HighAvailabilityServices haServices,
			final LibraryCacheManager.ClassLoaderLease classLoaderLease,
			final Executor executor,
			final FatalErrorHandler fatalErrorHandler) throws Exception {

		this.resultFuture = new CompletableFuture<>();
		this.terminationFuture = new CompletableFuture<>();
		this.leadershipOperation = CompletableFuture.completedFuture(null);

		this.jobGraph = checkNotNull(jobGraph);
		this.classLoaderLease = checkNotNull(classLoaderLease);
		this.executor = checkNotNull(executor);
		this.fatalErrorHandler = checkNotNull(fatalErrorHandler);

		checkArgument(jobGraph.getNumberOfVertices() > 0, "The given job is empty");

		// libraries and class loader first
		final ClassLoader userCodeLoader;
			userCodeLoader = classLoaderLease.getOrResolveClassLoader(
				jobGraph.getUserJarBlobKeys(),
				jobGraph.getClasspaths());

		// high availability services next
		this.runningJobsRegistry = haServices.getRunningJobsRegistry();
		this.leaderElectionService = haServices.getJobManagerLeaderElectionService(jobGraph.getJobID());

		this.leaderGatewayFuture = new CompletableFuture<>();

		// now start the JobManager
		this.jobMasterService = jobMasterFactory.createJobMasterService(jobGraph, this, userCodeLoader);
	}

	//----------------------------------------------------------------------------------------------
	// Getter
	//----------------------------------------------------------------------------------------------

	@Override
	public CompletableFuture<JobMasterGateway> getJobMasterGateway() {
		return leaderGatewayFuture;
	}

	@Override
	public CompletableFuture<ArchivedExecutionGraph> getResultFuture() {
		return resultFuture;
	}

	@Override
	public JobID getJobID() {
		return jobGraph.getJobID();
	}

	//----------------------------------------------------------------------------------------------
	// Lifecycle management
	//----------------------------------------------------------------------------------------------

	@Override
	public void start() throws Exception {
			leaderElectionService.start(this);
	}

	@Override
	public CompletableFuture<Void> closeAsync() {
		synchronized (lock) {
			if (!shutdown) {
				shutdown = true;

				setNewLeaderGatewayFuture();
				leaderGatewayFuture.completeExceptionally(new FlinkException("JobMaster has been shut down."));

				final CompletableFuture<Void> jobManagerTerminationFuture = jobMasterService.closeAsync();

				jobManagerTerminationFuture.whenComplete(
					(Void ignored, Throwable throwable) -> {
							leaderElectionService.stop();

						classLoaderLease.release();

						resultFuture.completeExceptionally(new JobNotFinishedException(jobGraph.getJobID()));

						if (throwable != null) {
							terminationFuture.completeExceptionally(
								new FlinkException("Could not properly shut down the JobManagerRunner", throwable));
						} else {
							terminationFuture.complete(null);
						}
					});
			}

			return terminationFuture;
		}
	}

	//----------------------------------------------------------------------------------------------
	// Result and error handling methods
	//----------------------------------------------------------------------------------------------

	/**
	 * Job completion notification triggered by JobManager.
	 */
	@Override
	public void jobReachedGloballyTerminalState(ArchivedExecutionGraph executionGraph) {
		unregisterJobFromHighAvailability();
		// complete the result future with the terminal execution graph
		resultFuture.complete(executionGraph);
	}

	/**
	 * Job completion notification triggered by self.
	 */
	@Override
	public void jobFinishedByOther() {
		resultFuture.completeExceptionally(new JobNotFinishedException(jobGraph.getJobID()));
	}

	@Override
	public void jobMasterFailed(Throwable cause) {
		handleJobManagerRunnerError(cause);
	}

	private void handleJobManagerRunnerError(Throwable cause) {
		if (ExceptionUtils.isJvmFatalError(cause)) {
			fatalErrorHandler.onFatalError(cause);
		} else {
			resultFuture.completeExceptionally(cause);
		}
	}

	/**
	 * Marks this runner's job as not running. Other JobManager will not recover the job
	 * after this call.
	 *
	 * <p>This method never throws an exception.
	 */
	private void unregisterJobFromHighAvailability() {
		try {
			runningJobsRegistry.setJobFinished(jobGraph.getJobID());
		}
		catch (Throwable t) {
			log.error("Could not un-register from high-availability services job {} ({})." +
					"Other JobManager's may attempt to recover it and re-execute it.",
					jobGraph.getName(), jobGraph.getJobID(), t);
		}
	}

	//----------------------------------------------------------------------------------------------
	// Leadership methods
	//----------------------------------------------------------------------------------------------

	@Override
	public void grantLeadership(final UUID leaderSessionID) {
		synchronized (lock) {
			if (shutdown) {
				return;
			}

			leadershipOperation = leadershipOperation.thenCompose(
				(ignored) -> {
					synchronized (lock) {
						return verifyJobSchedulingStatusAndStartJobManager(leaderSessionID);
					}
				});

			handleException(leadershipOperation, "Could not start the job manager.");
		}
	}

	private CompletableFuture<Void> verifyJobSchedulingStatusAndStartJobManager(UUID leaderSessionId) {
		final CompletableFuture<JobSchedulingStatus> jobSchedulingStatusFuture = getJobSchedulingStatus();

		return jobSchedulingStatusFuture.thenCompose(
			jobSchedulingStatus -> {
				if (jobSchedulingStatus == JobSchedulingStatus.DONE) {
					return jobAlreadyDone();
				} else {
					return startJobMaster(leaderSessionId);
				}
			});
	}

	private CompletionStage<Void> startJobMaster(UUID leaderSessionId) {
		log.info("JobManager runner for job {} ({}) was granted leadership with session id {} at {}.",
			jobGraph.getName(), jobGraph.getJobID(), leaderSessionId, jobMasterService.getAddress());

			runningJobsRegistry.setJobRunning(jobGraph.getJobID());

		final CompletableFuture<Acknowledge> startFuture;
			startFuture = jobMasterService.start(new JobMasterId(leaderSessionId));

		final CompletableFuture<JobMasterGateway> currentLeaderGatewayFuture = leaderGatewayFuture;
		return startFuture.thenAcceptAsync(
			(Acknowledge ack) -> confirmLeaderSessionIdIfStillLeader(
				leaderSessionId,
				jobMasterService.getAddress(),
				currentLeaderGatewayFuture),
			executor);
	}

	@Nonnull
	private CompletionStage<Void> jobAlreadyDone() {
		jobFinishedByOther();
		return CompletableFuture.completedFuture(null);
	}

	private CompletableFuture<JobSchedulingStatus> getJobSchedulingStatus() {
			JobSchedulingStatus jobSchedulingStatus = runningJobsRegistry.getJobSchedulingStatus(jobGraph.getJobID());
			return CompletableFuture.completedFuture(jobSchedulingStatus);
	}

	private void confirmLeaderSessionIdIfStillLeader(
			UUID leaderSessionId,
			String leaderAddress,
			CompletableFuture<JobMasterGateway> currentLeaderGatewayFuture) {

		if (leaderElectionService.hasLeadership(leaderSessionId)) {
			currentLeaderGatewayFuture.complete(jobMasterService.getGateway());
			leaderElectionService.confirmLeadership(leaderSessionId, leaderAddress);
		}
	}

	@Override
	public void revokeLeadership() {
		synchronized (lock) {
			if (shutdown) {
				return;
			}

			leadershipOperation = leadershipOperation.thenCompose(
				(ignored) -> {
					synchronized (lock) {
						return revokeJobMasterLeadership();
					}
				});

			handleException(leadershipOperation, "Could not suspend the job manager.");
		}
	}

	private CompletableFuture<Void> revokeJobMasterLeadership() {
		log.info("JobManager for job {} ({}) at {} was revoked leadership.",
			jobGraph.getName(), jobGraph.getJobID(), jobMasterService.getAddress());

		setNewLeaderGatewayFuture();

		return jobMasterService
			.suspend(new FlinkException("JobManager is no longer the leader."))
			.thenApply(FunctionUtils.nullFn());
	}

	private void handleException(CompletableFuture<Void> leadershipOperation, String message) {
		leadershipOperation.whenComplete(
			(ignored, throwable) -> {
				if (throwable != null) {
					handleJobManagerRunnerError(new FlinkException(message, throwable));
				}
			});
	}

	private void setNewLeaderGatewayFuture() {
		final CompletableFuture<JobMasterGateway> oldLeaderGatewayFuture = leaderGatewayFuture;

		leaderGatewayFuture = new CompletableFuture<>();

		if (!oldLeaderGatewayFuture.isDone()) {
			leaderGatewayFuture.whenComplete(
				(JobMasterGateway jobMasterGateway, Throwable throwable) -> {
					if (throwable != null) {
						oldLeaderGatewayFuture.completeExceptionally(throwable);
					} else {
						oldLeaderGatewayFuture.complete(jobMasterGateway);
					}
				});
		}
	}

	@Override
	public String getDescription() {
		return jobMasterService.getAddress();
	}

	@Override
	public void handleError(Exception exception) {
		handleJobManagerRunnerError(exception);
	}

	//----------------------------------------------------------------------------------------------
	// Testing
	//----------------------------------------------------------------------------------------------

	@VisibleForTesting
	boolean isShutdown() {
		return shutdown;
	}
}
