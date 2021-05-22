package org.apache.flink.runtime.entrypoint;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.ClusterOptions;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ConfigurationUtils;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.configuration.WebOptions;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.plugin.PluginManager;
import org.apache.flink.core.plugin.PluginUtils;
import org.apache.flink.runtime.blob.BlobServer;
import org.apache.flink.runtime.clusterframework.ApplicationStatus;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.concurrent.ScheduledExecutor;
import org.apache.flink.runtime.dispatcher.ArchivedExecutionGraphStore;
import org.apache.flink.runtime.dispatcher.MiniDispatcher;
import org.apache.flink.runtime.entrypoint.component.DispatcherResourceManagerComponent;
import org.apache.flink.runtime.entrypoint.component.DispatcherResourceManagerComponentFactory;
import org.apache.flink.runtime.entrypoint.parser.CommandLineParser;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServicesUtils;
import org.apache.flink.runtime.metrics.MetricRegistryConfiguration;
import org.apache.flink.runtime.metrics.MetricRegistryImpl;
import org.apache.flink.runtime.metrics.ReporterSetup;
import org.apache.flink.runtime.metrics.groups.ProcessMetricGroup;
import org.apache.flink.runtime.metrics.util.MetricUtils;
import org.apache.flink.runtime.resourcemanager.ResourceManager;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.RpcUtils;
import org.apache.flink.runtime.rpc.akka.AkkaRpcServiceUtils;
import org.apache.flink.runtime.security.SecurityConfiguration;
import org.apache.flink.runtime.security.SecurityUtils;
import org.apache.flink.runtime.security.contexts.SecurityContext;
import org.apache.flink.runtime.util.ClusterEntrypointUtils;
import org.apache.flink.runtime.util.ExecutorThreadFactory;
import org.apache.flink.runtime.util.ZooKeeperUtils;
import org.apache.flink.runtime.webmonitor.retriever.impl.RpcMetricQueryServiceRetriever;
import org.apache.flink.util.AutoCloseableAsync;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.ExecutorUtils;
import org.apache.flink.util.FileUtils;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.ShutdownHookUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.UndeclaredThrowableException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.flink.runtime.security.ExitTrappingSecurityManager.replaceGracefulExitWithHaltIfConfigured;

/**
 * Base class for the Flink cluster entry points.
 *
 * <p>Specialization of this class can be used for the session mode and the per-job mode
 */
public abstract class ClusterEntrypoint implements AutoCloseableAsync, FatalErrorHandler {

	public static final ConfigOption<String> EXECUTION_MODE = ConfigOptions
		.key("internal.cluster.execution-mode")
		.defaultValue(ExecutionMode.NORMAL.toString());

	protected static final int STARTUP_FAILURE_RETURN_CODE = 1;
	protected static final int RUNTIME_FAILURE_RETURN_CODE = 2;

	private static final Time INITIALIZATION_SHUTDOWN_TIMEOUT = Time.seconds(30L);

	/** The lock to guard startup / shutdown / manipulation methods. */
	private final Object lock = new Object();

	private final Configuration configuration;

	private final CompletableFuture<ApplicationStatus> terminationFuture;

	private final AtomicBoolean isShutDown = new AtomicBoolean(false);

	@GuardedBy("lock")
	private DispatcherResourceManagerComponent clusterComponent;

	@GuardedBy("lock")
	private MetricRegistryImpl metricRegistry;

	@GuardedBy("lock")
	private ProcessMetricGroup processMetricGroup;

	@GuardedBy("lock")
	private HighAvailabilityServices haServices;

	@GuardedBy("lock")
	private BlobServer blobServer;

	@GuardedBy("lock")
	private HeartbeatServices heartbeatServices;

	@GuardedBy("lock")
	private RpcService commonRpcService;

	@GuardedBy("lock")
	private ExecutorService ioExecutor;

	private ArchivedExecutionGraphStore archivedExecutionGraphStore;

	private final Thread shutDownHook;

	protected ClusterEntrypoint(Configuration configuration) {
		this.configuration = generateClusterConfiguration(configuration);
		this.terminationFuture = new CompletableFuture<>();

		shutDownHook = ShutdownHookUtil.addShutdownHook(this::cleanupDirectories, getClass().getSimpleName(), LOG);
	}

	public CompletableFuture<ApplicationStatus> getTerminationFuture() {
		return terminationFuture;
	}

	public void startCluster() throws ClusterEntrypointException {

			replaceGracefulExitWithHaltIfConfigured(configuration);
			PluginManager pluginManager = PluginUtils.createPluginManagerFromRootFolder(configuration);
			configureFileSystems(configuration, pluginManager);

			SecurityContext securityContext = installSecurityContext(configuration);

			securityContext.runSecured((Callable<Void>) () -> {
				runCluster(configuration, pluginManager);

				return null;
			});
		
	}

	private void configureFileSystems(Configuration configuration, PluginManager pluginManager) {
		FileSystem.initialize(configuration, pluginManager);
	}

	private SecurityContext installSecurityContext(Configuration configuration) throws Exception {

		SecurityUtils.install(new SecurityConfiguration(configuration));

		return SecurityUtils.getInstalledContext();
	}

	private void runCluster(Configuration configuration, PluginManager pluginManager) throws Exception {
		synchronized (lock) {

			initializeServices(configuration, pluginManager);

			// write host information into configuration
			configuration.setString(JobManagerOptions.ADDRESS, commonRpcService.getAddress());
			configuration.setInteger(JobManagerOptions.PORT, commonRpcService.getPort());

			final DispatcherResourceManagerComponentFactory dispatcherResourceManagerComponentFactory = createDispatcherResourceManagerComponentFactory(configuration);

			clusterComponent = dispatcherResourceManagerComponentFactory.create(
				configuration,
				ioExecutor,
				commonRpcService,
				haServices,
				blobServer,
				heartbeatServices,
				metricRegistry,
				archivedExecutionGraphStore,
				new RpcMetricQueryServiceRetriever(metricRegistry.getMetricQueryServiceRpcService()),
				this);

			clusterComponent.getShutDownFuture().whenComplete(
				(ApplicationStatus applicationStatus, Throwable throwable) -> {
					if (throwable != null) {
						shutDownAsync(
							ApplicationStatus.UNKNOWN,
							ExceptionUtils.stringifyException(throwable),
							false);
					} else {
						// This is the general shutdown path. If a separate more specific shutdown was
						// already triggered, this will do nothing
						shutDownAsync(
							applicationStatus,
							null,
							true);
					}
				});
		}
	}

	protected void initializeServices(Configuration configuration, PluginManager pluginManager) throws Exception {

		synchronized (lock) {
			commonRpcService = AkkaRpcServiceUtils.createRemoteRpcService(
				configuration,
				configuration.getString(JobManagerOptions.ADDRESS),
				getRPCPortRange(configuration),
				configuration.getString(JobManagerOptions.BIND_HOST),
				configuration.getOptional(JobManagerOptions.RPC_BIND_PORT));

			// update the configuration used to create the high availability services
			configuration.setString(JobManagerOptions.ADDRESS, commonRpcService.getAddress());
			configuration.setInteger(JobManagerOptions.PORT, commonRpcService.getPort());

			ioExecutor = Executors.newFixedThreadPool(
				ClusterEntrypointUtils.getPoolSize(configuration),
				new ExecutorThreadFactory("cluster-io"));
			haServices = createHaServices(configuration, ioExecutor);
			blobServer = new BlobServer(configuration, haServices.createBlobStore());
			blobServer.start();
			heartbeatServices = createHeartbeatServices(configuration);
			metricRegistry = createMetricRegistry(configuration, pluginManager);

			final RpcService metricQueryServiceRpcService = MetricUtils.startRemoteMetricsRpcService(configuration, commonRpcService.getAddress());
			metricRegistry.startQueryService(metricQueryServiceRpcService, null);

			final String hostname = RpcUtils.getHostname(commonRpcService);

			processMetricGroup = MetricUtils.instantiateProcessMetricGroup(
				metricRegistry,
				hostname,
				ConfigurationUtils.getSystemResourceMetricsProbingInterval(configuration));

			archivedExecutionGraphStore = createSerializableExecutionGraphStore(configuration, commonRpcService.getScheduledExecutor());
		}
	}

	/**
	 * Returns the port range for the common {@link RpcService}.
	 *
	 * @param configuration to extract the port range from
	 * @return Port range for the common {@link RpcService}
	 */
	protected String getRPCPortRange(Configuration configuration) {
		if (ZooKeeperUtils.isZooKeeperRecoveryMode(configuration)) {
			return configuration.getString(HighAvailabilityOptions.HA_JOB_MANAGER_PORT_RANGE);
		} else {
			return String.valueOf(configuration.getInteger(JobManagerOptions.PORT));
		}
	}

	protected HighAvailabilityServices createHaServices(
		Configuration configuration,
		Executor executor) throws Exception {
		return HighAvailabilityServicesUtils.createHighAvailabilityServices(
			configuration,
			executor,
			HighAvailabilityServicesUtils.AddressResolution.NO_ADDRESS_RESOLUTION);
	}

	protected HeartbeatServices createHeartbeatServices(Configuration configuration) {
		return HeartbeatServices.fromConfiguration(configuration);
	}

	protected MetricRegistryImpl createMetricRegistry(Configuration configuration, PluginManager pluginManager) {
		return new MetricRegistryImpl(
			MetricRegistryConfiguration.fromConfiguration(configuration),
			ReporterSetup.fromConfiguration(configuration, pluginManager));
	}

	@Override
	public CompletableFuture<Void> closeAsync() {
		return shutDownAsync(
			ApplicationStatus.UNKNOWN,
			"Cluster entrypoint has been closed externally.",
			true).thenAccept(ignored -> {});
	}

	protected CompletableFuture<Void> stopClusterServices(boolean cleanupHaData) {
		final long shutdownTimeout = configuration.getLong(ClusterOptions.CLUSTER_SERVICES_SHUTDOWN_TIMEOUT);

		synchronized (lock) {
			Throwable exception = null;

			final Collection<CompletableFuture<Void>> terminationFutures = new ArrayList<>(3);

					blobServer.close();

					if (cleanupHaData) {
						haServices.closeAndCleanupAllData();
					} else {
						haServices.close();
					}

					archivedExecutionGraphStore.close();

				processMetricGroup.close();

				terminationFutures.add(metricRegistry.shutdown());

				terminationFutures.add(ExecutorUtils.nonBlockingShutdown(shutdownTimeout, TimeUnit.MILLISECONDS, ioExecutor));

				terminationFutures.add(commonRpcService.stopService());

				terminationFutures.add(FutureUtils.completedExceptionally(exception));

			return FutureUtils.completeAll(terminationFutures);
		}
	}

	@Override
	public void onFatalError(Throwable exception) {
		Throwable enrichedException = ClusterEntryPointExceptionUtils.tryEnrichClusterEntryPointError(exception);

		System.exit(RUNTIME_FAILURE_RETURN_CODE);
	}

	// --------------------------------------------------
	// Internal methods
	// --------------------------------------------------

	private Configuration generateClusterConfiguration(Configuration configuration) {
		final Configuration resultConfiguration = new Configuration(Preconditions.checkNotNull(configuration));

		final String webTmpDir = configuration.getString(WebOptions.TMP_DIR);
		final File uniqueWebTmpDir = new File(webTmpDir, "flink-web-" + UUID.randomUUID());

		resultConfiguration.setString(WebOptions.TMP_DIR, uniqueWebTmpDir.getAbsolutePath());

		return resultConfiguration;
	}

	private CompletableFuture<ApplicationStatus> shutDownAsync(
			ApplicationStatus applicationStatus,
			@Nullable String diagnostics,
			boolean cleanupHaData) {
		if (isShutDown.compareAndSet(false, true)) {

			final CompletableFuture<Void> shutDownApplicationFuture = closeClusterComponent(applicationStatus, diagnostics);

			final CompletableFuture<Void> serviceShutdownFuture = FutureUtils.composeAfterwards(
				shutDownApplicationFuture,
				() -> stopClusterServices(cleanupHaData));

			final CompletableFuture<Void> cleanupDirectoriesFuture = FutureUtils.runAfterwards(
				serviceShutdownFuture,
				this::cleanupDirectories);

			cleanupDirectoriesFuture.whenComplete(
				(Void ignored2, Throwable serviceThrowable) -> {
					if (serviceThrowable != null) {
						terminationFuture.completeExceptionally(serviceThrowable);
					} else {
						terminationFuture.complete(applicationStatus);
					}
				});
		}

		return terminationFuture;
	}

	/**
	 * Deregister the Flink application from the resource management system by signalling
	 * the {@link ResourceManager}.
	 *
	 * @param applicationStatus to terminate the application with
	 * @param diagnostics additional information about the shut down, can be {@code null}
	 * @return Future which is completed once the shut down
	 */
	private CompletableFuture<Void> closeClusterComponent(ApplicationStatus applicationStatus, @Nullable String diagnostics) {
		synchronized (lock) {
			if (clusterComponent != null) {
				return clusterComponent.deregisterApplicationAndClose(applicationStatus, diagnostics);
			} else {
				return CompletableFuture.completedFuture(null);
			}
		}
	}

	/**
	 * Clean up of temporary directories created by the {@link ClusterEntrypoint}.
	 *
	 * @throws IOException if the temporary directories could not be cleaned up
	 */
	private void cleanupDirectories() throws IOException {
		ShutdownHookUtil.removeShutdownHook(shutDownHook, getClass().getSimpleName(), LOG);

		final String webTmpDir = configuration.getString(WebOptions.TMP_DIR);

		FileUtils.deleteDirectory(new File(webTmpDir));
	}

	// --------------------------------------------------
	// Abstract methods
	// --------------------------------------------------

	protected abstract DispatcherResourceManagerComponentFactory createDispatcherResourceManagerComponentFactory(Configuration configuration) throws IOException;

	protected abstract ArchivedExecutionGraphStore createSerializableExecutionGraphStore(
		Configuration configuration,
		ScheduledExecutor scheduledExecutor) throws IOException;

	protected static EntrypointClusterConfiguration parseArguments(String[] args) throws FlinkParseException {
		final CommandLineParser<EntrypointClusterConfiguration> clusterConfigurationParser = new CommandLineParser<>(new EntrypointClusterConfigurationParserFactory());

		return clusterConfigurationParser.parse(args);
	}

	protected static Configuration loadConfiguration(EntrypointClusterConfiguration entrypointClusterConfiguration) {
		final Configuration dynamicProperties = ConfigurationUtils.createConfiguration(entrypointClusterConfiguration.getDynamicProperties());
		final Configuration configuration = GlobalConfiguration.loadConfiguration(entrypointClusterConfiguration.getConfigDir(), dynamicProperties);

		final int restPort = entrypointClusterConfiguration.getRestPort();

		if (restPort >= 0) {
			configuration.setInteger(RestOptions.PORT, restPort);
		}

		final String hostname = entrypointClusterConfiguration.getHostname();

		if (hostname != null) {
			configuration.setString(JobManagerOptions.ADDRESS, hostname);
		}

		return configuration;
	}

	// --------------------------------------------------
	// Helper methods
	// --------------------------------------------------

	public static void runClusterEntrypoint(ClusterEntrypoint clusterEntrypoint) {

		final String clusterEntrypointName = clusterEntrypoint.getClass().getSimpleName();
			clusterEntrypoint.startCluster();

		clusterEntrypoint.getTerminationFuture().whenComplete((applicationStatus, throwable) -> {
			final int returnCode;

			if (throwable != null) {
				returnCode = RUNTIME_FAILURE_RETURN_CODE;
			} else {
				returnCode = applicationStatus.processExitCode();
			}

			System.exit(returnCode);
		});
	}

	/**
	 * Execution mode of the {@link MiniDispatcher}.
	 */
	public enum ExecutionMode {
		/**
		 * Waits until the job result has been served.
		 */
		NORMAL,

		/**
		 * Directly stops after the job has finished.
		 */
		DETACHED
	}
}
