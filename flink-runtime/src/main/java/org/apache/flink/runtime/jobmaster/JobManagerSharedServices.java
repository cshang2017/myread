package org.apache.flink.runtime.jobmaster;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.configuration.WebOptions;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.blob.BlobServer;
import org.apache.flink.runtime.blob.BlobWriter;
import org.apache.flink.runtime.execution.librarycache.BlobLibraryCacheManager;
import org.apache.flink.runtime.execution.librarycache.FlinkUserCodeClassLoaders;
import org.apache.flink.runtime.execution.librarycache.LibraryCacheManager;
import org.apache.flink.runtime.rest.handler.legacy.backpressure.BackPressureRequestCoordinator;
import org.apache.flink.runtime.rest.handler.legacy.backpressure.BackPressureStatsTracker;
import org.apache.flink.runtime.rest.handler.legacy.backpressure.BackPressureStatsTrackerImpl;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.util.ExecutorThreadFactory;
import org.apache.flink.runtime.util.Hardware;
import org.apache.flink.util.ExceptionUtils;

import javax.annotation.Nonnull;

import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Utility class which holds all auxiliary shared services used by the {@link JobMaster}.
 * Consequently, the {@link JobMaster} should never shut these services down.
 */
public class JobManagerSharedServices {

	private final ScheduledExecutorService scheduledExecutorService;

	private final LibraryCacheManager libraryCacheManager;

	private final BackPressureRequestCoordinator backPressureSampleCoordinator;

	private final BackPressureStatsTracker backPressureStatsTracker;

	@Nonnull
	private final BlobWriter blobWriter;

	public JobManagerSharedServices(
			ScheduledExecutorService scheduledExecutorService,
			LibraryCacheManager libraryCacheManager,
			BackPressureRequestCoordinator backPressureSampleCoordinator,
			BackPressureStatsTracker backPressureStatsTracker,
			@Nonnull BlobWriter blobWriter) {

		this.scheduledExecutorService = checkNotNull(scheduledExecutorService);
		this.libraryCacheManager = checkNotNull(libraryCacheManager);
		this.backPressureSampleCoordinator = checkNotNull(backPressureSampleCoordinator);
		this.backPressureStatsTracker = checkNotNull(backPressureStatsTracker);
		this.blobWriter = blobWriter;
	}

	public ScheduledExecutorService getScheduledExecutorService() {
		return scheduledExecutorService;
	}

	public LibraryCacheManager getLibraryCacheManager() {
		return libraryCacheManager;
	}

	public BackPressureStatsTracker getBackPressureStatsTracker() {
		return backPressureStatsTracker;
	}

	@Nonnull
	public BlobWriter getBlobWriter() {
		return blobWriter;
	}

	/**
	 * Shutdown the {@link JobMaster} services.
	 *
	 * <p>This method makes sure all services are closed or shut down, even when an exception occurred
	 * in the shutdown of one component. The first encountered exception is thrown, with successive
	 * exceptions added as suppressed exceptions.
	 *
	 * @throws Exception The first Exception encountered during shutdown.
	 */
	public void shutdown() throws Exception {
		Throwable firstException = null;

			scheduledExecutorService.shutdownNow();

		libraryCacheManager.shutdown();
		backPressureSampleCoordinator.shutDown();
		backPressureStatsTracker.shutDown();

		if (firstException != null) {
			ExceptionUtils.rethrowException(firstException, "Error while shutting down JobManager services");
		}
	}

	// ------------------------------------------------------------------------
	//  Creating the components from a configuration
	// ------------------------------------------------------------------------

	public static JobManagerSharedServices fromConfiguration(
			Configuration config,
			BlobServer blobServer,
			FatalErrorHandler fatalErrorHandler) {

		checkNotNull(config);
		checkNotNull(blobServer);

		final String classLoaderResolveOrder =
			config.getString(CoreOptions.CLASSLOADER_RESOLVE_ORDER);

		final String[] alwaysParentFirstLoaderPatterns = CoreOptions.getParentFirstLoaderPatterns(config);

		final boolean failOnJvmMetaspaceOomError = config.getBoolean(CoreOptions.FAIL_ON_USER_CLASS_LOADING_METASPACE_OOM);
		final BlobLibraryCacheManager libraryCacheManager =
			new BlobLibraryCacheManager(
				blobServer,
				BlobLibraryCacheManager.defaultClassLoaderFactory(
					FlinkUserCodeClassLoaders.ResolveOrder.fromString(classLoaderResolveOrder),
					alwaysParentFirstLoaderPatterns,
					failOnJvmMetaspaceOomError ? fatalErrorHandler : null));

		final Duration akkaTimeout;
		try {
			akkaTimeout = AkkaUtils.getTimeout(config);
		} catch (NumberFormatException e) {
			throw new IllegalConfigurationException(AkkaUtils.formatDurationParsingErrorMessage());
		}

		final ScheduledExecutorService futureExecutor = Executors.newScheduledThreadPool(
				Hardware.getNumberCPUCores(),
				new ExecutorThreadFactory("jobmanager-future"));

		final int numSamples = config.getInteger(WebOptions.BACKPRESSURE_NUM_SAMPLES);
		final long delayBetweenSamples = config.getInteger(WebOptions.BACKPRESSURE_DELAY);
		final BackPressureRequestCoordinator coordinator = new BackPressureRequestCoordinator(
			futureExecutor,
			akkaTimeout.toMillis() + numSamples * delayBetweenSamples);

		final int cleanUpInterval = config.getInteger(WebOptions.BACKPRESSURE_CLEANUP_INTERVAL);
		final BackPressureStatsTrackerImpl backPressureStatsTracker = new BackPressureStatsTrackerImpl(
			coordinator,
			cleanUpInterval,
			config.getInteger(WebOptions.BACKPRESSURE_REFRESH_INTERVAL));

		futureExecutor.scheduleWithFixedDelay(
			backPressureStatsTracker::cleanUpOperatorStatsCache,
			cleanUpInterval,
			cleanUpInterval,
			TimeUnit.MILLISECONDS);

		return new JobManagerSharedServices(
			futureExecutor,
			libraryCacheManager,
			coordinator,
			backPressureStatsTracker,
			blobServer);
	}
}
