
package org.apache.flink.client.cli;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.InvalidProgramException;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.dag.Pipeline;
import org.apache.flink.client.ClientUtils;
import org.apache.flink.client.FlinkPipelineTranslationUtil;
import org.apache.flink.client.deployment.ClusterClientFactory;
import org.apache.flink.client.deployment.ClusterClientServiceLoader;
import org.apache.flink.client.deployment.ClusterDescriptor;
import org.apache.flink.client.deployment.DefaultClusterClientServiceLoader;
import org.apache.flink.client.deployment.application.ApplicationConfiguration;
import org.apache.flink.client.deployment.application.cli.ApplicationClusterDeployer;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.client.program.PackagedProgram;
import org.apache.flink.client.program.PackagedProgramUtils;
import org.apache.flink.client.program.ProgramInvocationException;
import org.apache.flink.client.program.ProgramMissingJobException;
import org.apache.flink.client.program.ProgramParametrizationException;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.core.execution.DefaultExecutorServiceLoader;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.plugin.PluginUtils;
import org.apache.flink.runtime.client.JobStatusMessage;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.security.SecurityConfiguration;
import org.apache.flink.runtime.security.SecurityUtils;
import org.apache.flink.runtime.util.EnvironmentInformation;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.lang.reflect.Constructor;
import java.lang.reflect.UndeclaredThrowableException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.apache.flink.client.cli.CliFrontendParser.HELP_OPTION;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Implementation of a simple command line frontend for executing programs.
 */
public class CliFrontend {

	// actions
	private static final String ACTION_RUN = "run";
	private static final String ACTION_RUN_APPLICATION = "run-application";
	private static final String ACTION_INFO = "info";
	private static final String ACTION_LIST = "list";
	private static final String ACTION_CANCEL = "cancel";
	private static final String ACTION_STOP = "stop";
	private static final String ACTION_SAVEPOINT = "savepoint";

	// configuration dir parameters
	private static final String CONFIG_DIRECTORY_FALLBACK_1 = "../conf";
	private static final String CONFIG_DIRECTORY_FALLBACK_2 = "conf";


	private final Configuration configuration;
	private final List<CustomCommandLine> customCommandLines;

	private final Options customCommandLineOptions;
	private final Duration clientTimeout;
	private final int defaultParallelism;
	private final ClusterClientServiceLoader clusterClientServiceLoader;

	public CliFrontend(
			Configuration configuration,
			List<CustomCommandLine> customCommandLines) {
		this(configuration, new DefaultClusterClientServiceLoader(), customCommandLines);
	}

	public CliFrontend(
			Configuration configuration,
			ClusterClientServiceLoader clusterClientServiceLoader,
			List<CustomCommandLine> customCommandLines) {
		this.configuration = checkNotNull(configuration);
		this.customCommandLines = checkNotNull(customCommandLines);
		this.clusterClientServiceLoader = checkNotNull(clusterClientServiceLoader);

		FileSystem.initialize(configuration, PluginUtils.createPluginManagerFromRootFolder(configuration));

		this.customCommandLineOptions = new Options();

		for (CustomCommandLine customCommandLine : customCommandLines) {
			customCommandLine.addGeneralOptions(customCommandLineOptions);
			customCommandLine.addRunOptions(customCommandLineOptions);
		}

		this.clientTimeout = configuration.get(ClientOptions.CLIENT_TIMEOUT);
		this.defaultParallelism = configuration.getInteger(CoreOptions.DEFAULT_PARALLELISM);
	}


	public Configuration getConfiguration() {

		Configuration copiedConfiguration = new Configuration();
		copiedConfiguration.addAll(configuration);

		return copiedConfiguration;
	}

	public Options getCustomCommandLineOptions() {
		return customCommandLineOptions;
	}

	protected void runApplication(String[] args) {

		final Options commandOptions = CliFrontendParser.getRunCommandOptions();
		final CommandLine commandLine = getCommandLine(commandOptions, args, true);

		final CustomCommandLine activeCommandLine =
				validateAndGetActiveCommandLine(checkNotNull(commandLine));

		final ProgramOptions programOptions = new ProgramOptions(commandLine);

		final ApplicationDeployer deployer =
				new ApplicationClusterDeployer(clusterClientServiceLoader);

		programOptions.validate();
		URI uri = PackagedProgramUtils.resolveURI(programOptions.getJarFilePath());
		Configuration effectiveConfiguration = getEffectiveConfiguration(
				activeCommandLine, commandLine, programOptions, Collections.singletonList(uri.toString()));

		ApplicationConfiguration applicationConfiguration =
				new ApplicationConfiguration(programOptions.getProgramArgs(), programOptions.getEntryPointClassName());

		deployer.run(effectiveConfiguration, applicationConfiguration);
	}


	protected void run(String[] args) {

		final Options commandOptions = CliFrontendParser.getRunCommandOptions();
		final CommandLine commandLine = getCommandLine(commandOptions, args, true);

		final CustomCommandLine activeCommandLine =
				validateAndGetActiveCommandLine(checkNotNull(commandLine));

		final ProgramOptions programOptions = ProgramOptions.create(commandLine);

		final PackagedProgram program =
				getPackagedProgram(programOptions);

		final List<URL> jobJars = program.getJobJarAndDependencies();
		final Configuration effectiveConfiguration = getEffectiveConfiguration(
				activeCommandLine, commandLine, programOptions, jobJars);

		executeProgram(effectiveConfiguration, program);
		program.deleteExtractedLibraries();
	}

	private PackagedProgram getPackagedProgram(ProgramOptions programOptions) throws ProgramInvocationException, CliArgsException {
		PackagedProgram program = buildProgram(programOptions);
		return program;
	}

	private <T> Configuration getEffectiveConfiguration(
			final CustomCommandLine activeCustomCommandLine,
			final CommandLine commandLine,
			final ProgramOptions programOptions,
			final List<T> jobJars) throws FlinkException {

		ExecutionConfigAccessor executionParameters = ExecutionConfigAccessor.fromProgramOptions(
				programOptions, jobJars);

		final Configuration executorConfig = activeCustomCommandLine
				.applyCommandLineOptionsToConfiguration(commandLine);

		final Configuration effectiveConfiguration = new Configuration(executorConfig);

		executionParameters.applyToConfiguration(effectiveConfiguration);
		return effectiveConfiguration;
	}

	protected void info(String[] args) throws Exception {

		Options commandOptions = CliFrontendParser.getInfoCommandOptions();
		CommandLine commandLine = CliFrontendParser.parse(commandOptions, args, true);
		ProgramOptions programOptions = ProgramOptions.create(commandLine);

		PackagedProgram program = buildProgram(programOptions);

		int parallelism = programOptions.getParallelism();
		if (ExecutionConfig.PARALLELISM_DEFAULT == parallelism) {
			parallelism = defaultParallelism;
		}

		CustomCommandLine activeCommandLine =
				validateAndGetActiveCommandLine(commandLine);

		Configuration effectiveConfiguration = getEffectiveConfiguration(
				activeCommandLine, commandLine, programOptions, program.getJobJarAndDependencies());

		Pipeline pipeline = PackagedProgramUtils.getPipelineFromProgram(program, effectiveConfiguration, parallelism, true);

		String jsonPlan = FlinkPipelineTranslationUtil.translateToJSONExecutionPlan(pipeline);
		System.out.println(jsonPlan);

		String description = program.getDescription();
		System.out.println(description);

		program.deleteExtractedLibraries();
	}

	protected void list(String[] args) throws Exception {

		final Options commandOptions = CliFrontendParser.getListCommandOptions();
		final CommandLine commandLine = getCommandLine(commandOptions, args, false);

		ListOptions listOptions = new ListOptions(commandLine);

		final boolean showRunning;
		final boolean showScheduled;
		final boolean showAll;

		// print running and scheduled jobs if not option supplied
		if (!listOptions.showRunning() && !listOptions.showScheduled() && !listOptions.showAll()) {
			showRunning = true;
			showScheduled = true;
			showAll = false;
		} else {
			showRunning = listOptions.showRunning();
			showScheduled = listOptions.showScheduled();
			showAll = listOptions.showAll();
		}

		final CustomCommandLine activeCommandLine = validateAndGetActiveCommandLine(commandLine);

		runClusterAction(
			activeCommandLine,
			commandLine,
			clusterClient -> listJobs(clusterClient, showRunning, showScheduled, showAll));

	}

	private <ClusterID> void listJobs(
			ClusterClient<ClusterID> clusterClient,
			boolean showRunning,
			boolean showScheduled,
			boolean showAll) {

		CompletableFuture<Collection<JobStatusMessage>> jobDetailsFuture = clusterClient.listJobs();

		Collection<JobStatusMessage> jobDetails = jobDetailsFuture.get();

		final List<JobStatusMessage> runningJobs = new ArrayList<>();
		final List<JobStatusMessage> scheduledJobs = new ArrayList<>();
		final List<JobStatusMessage> terminatedJobs = new ArrayList<>();
		jobDetails.forEach(details -> {
			if (details.getJobState() == JobStatus.CREATED) {
				scheduledJobs.add(details);
			} else if (!details.getJobState().isGloballyTerminalState()) {
				runningJobs.add(details);
			} else {
				terminatedJobs.add(details);
			}
		});

		if (showRunning || showAll) {
			printJobStatusMessages(runningJobs);
		}
		if (showScheduled || showAll) {
			printJobStatusMessages(scheduledJobs);
		}
		if (showAll) {
			printJobStatusMessages(terminatedJobs);
		}
	}

	private static void printJobStatusMessages(List<JobStatusMessage> jobs) {
		SimpleDateFormat dateFormat = new SimpleDateFormat("dd.MM.yyyy HH:mm:ss");
		Comparator<JobStatusMessage> startTimeComparator = (o1, o2) -> (int) (o1.getStartTime() - o2.getStartTime());
		Comparator<Map.Entry<JobStatus, List<JobStatusMessage>>> statusComparator =
			(o1, o2) -> String.CASE_INSENSITIVE_ORDER.compare(o1.getKey().toString(), o2.getKey().toString());

		Map<JobStatus, List<JobStatusMessage>> jobsByState = jobs.stream().collect(Collectors.groupingBy(JobStatusMessage::getJobState));

		jobsByState.entrySet().stream()
			.sorted(statusComparator)
			.map(Map.Entry::getValue).flatMap(List::stream).sorted(startTimeComparator)
			.forEachOrdered(job -> );
	}

	protected void stop(String[] args) throws Exception {

		final Options commandOptions = CliFrontendParser.getStopCommandOptions();
		final CommandLine commandLine = getCommandLine(commandOptions, args, false);

		final StopOptions stopOptions = new StopOptions(commandLine);

		final String[] cleanedArgs = stopOptions.getArgs();

		final String targetDirectory = stopOptions.hasSavepointFlag() && cleanedArgs.length > 0
				? stopOptions.getTargetDirectory()
				: null; // the default savepoint location is going to be used in this case.

		final JobID jobId = cleanedArgs.length != 0
				? parseJobId(cleanedArgs[0])
				: parseJobId(stopOptions.getTargetDirectory());

		final boolean advanceToEndOfEventTime = stopOptions.shouldAdvanceToEndOfEventTime();

		final CustomCommandLine activeCommandLine = validateAndGetActiveCommandLine(commandLine);
		runClusterAction(
			activeCommandLine,
			commandLine,
			clusterClient -> {
				String savepointPath = clusterClient.stopWithSavepoint(jobId, advanceToEndOfEventTime, targetDirectory).get(clientTimeout.toMillis(), TimeUnit.MILLISECONDS);
			});
	}

	protected void cancel(String[] args) throws Exception {

		final Options commandOptions = CliFrontendParser.getCancelCommandOptions();
		final CommandLine commandLine = getCommandLine(commandOptions, args, false);

		CancelOptions cancelOptions = new CancelOptions(commandLine);
		final CustomCommandLine activeCommandLine = validateAndGetActiveCommandLine(commandLine);

		final String[] cleanedArgs = cancelOptions.getArgs();

		if (cancelOptions.isWithSavepoint()) {

			final JobID jobId;
			final String targetDirectory;

			if (cleanedArgs.length > 0) {
				jobId = parseJobId(cleanedArgs[0]);
				targetDirectory = cancelOptions.getSavepointTargetDirectory();
			} else {
				jobId = parseJobId(cancelOptions.getSavepointTargetDirectory());
				targetDirectory = null;
			}

			runClusterAction(
				activeCommandLine,
				commandLine,
				clusterClient -> {
					String savepointPath = clusterClient.cancelWithSavepoint(jobId, targetDirectory).get(clientTimeout.toMillis(), TimeUnit.MILLISECONDS);
				});
		} else {
			JobID jobId = parseJobId(cleanedArgs[0]);

			runClusterAction(
				activeCommandLine,
				commandLine,
				clusterClient -> {
					clusterClient.cancel(jobId).get(clientTimeout.toMillis(), TimeUnit.MILLISECONDS);
				});
		}
	}

	public CommandLine getCommandLine(Options commandOptions, String[] args, boolean stopAtNonOptions) {
		final Options commandLineOptions = CliFrontendParser.mergeOptions(commandOptions, customCommandLineOptions);
		return CliFrontendParser.parse(commandLineOptions, args, stopAtNonOptions);
	}

	protected void savepoint(String[] args) {

		Options commandOptions = CliFrontendParser.getSavepointCommandOptions();

		Options commandLineOptions = CliFrontendParser.mergeOptions(commandOptions, customCommandLineOptions);

		final CommandLine commandLine = CliFrontendParser.parse(commandLineOptions, args, false);

		final SavepointOptions savepointOptions = new SavepointOptions(commandLine);

		final CustomCommandLine activeCommandLine = validateAndGetActiveCommandLine(commandLine);

		if (savepointOptions.isDispose()) {
			runClusterAction(
				activeCommandLine,
				commandLine,
				clusterClient -> disposeSavepoint(clusterClient, savepointOptions.getSavepointPath()));
		} else {
			String[] cleanedArgs = savepointOptions.getArgs();

			final JobID jobId;

			if (cleanedArgs.length >= 1) {
				String jobIdString = cleanedArgs[0];

				jobId = parseJobId(jobIdString);
			}

			final String savepointDirectory;
			if (cleanedArgs.length >= 2) {
				savepointDirectory = cleanedArgs[1];
			} else {
				savepointDirectory = null;
			}

			runClusterAction(
				activeCommandLine,
				commandLine,
				clusterClient -> triggerSavepoint(clusterClient, jobId, savepointDirectory));
		}

	}

	/**
	 * Sends a SavepointTriggerMessage to the job manager.
	 */
	private void triggerSavepoint(ClusterClient<?> clusterClient, JobID jobId, String savepointDirectory) {

		CompletableFuture<String> savepointPathFuture = clusterClient.triggerSavepoint(jobId, savepointDirectory);

		String savepointPath = savepointPathFuture.get(clientTimeout.toMillis(), TimeUnit.MILLISECONDS);
	}

	/**
	 * Sends a SavepointDisposalRequest to the job manager.
	 */
	private void disposeSavepoint(ClusterClient<?> clusterClient, String savepointPath) {

		final CompletableFuture<Acknowledge> disposeFuture = clusterClient.disposeSavepoint(savepointPath);

		disposeFuture.get(clientTimeout.toMillis(), TimeUnit.MILLISECONDS);
	}

	//  Interaction with programs and JobManager

	protected void executeProgram(Configuration configuration, PackagedProgram program) {
		ClientUtils.executeProgram(new DefaultExecutorServiceLoader(), configuration, program, false, false);
	}

	/**
	 * Creates a Packaged program from the given command line options.
	 *
	 * @return A PackagedProgram (upon success)
	 */
	PackagedProgram buildProgram(ProgramOptions runOptions) {
		runOptions.validate();

		String[] programArgs = runOptions.getProgramArgs();
		String jarFilePath = runOptions.getJarFilePath();
		List<URL> classpaths = runOptions.getClasspaths();

		// Get assembler class
		String entryPointClass = runOptions.getEntryPointClassName();
		File jarFile = jarFilePath != null ? getJarFile(jarFilePath) : null;

		return PackagedProgram.newBuilder()
			.setJarFile(jarFile)
			.setUserClassPaths(classpaths)
			.setEntryPointClassName(entryPointClass)
			.setConfiguration(configuration)
			.setSavepointRestoreSettings(runOptions.getSavepointRestoreSettings())
			.setArguments(programArgs)
			.build();
	}

	/**
	 * Gets the JAR file from the path.
	 *
	 * @param jarFilePath The path of JAR file
	 * @return The JAR file
	 * @throws FileNotFoundException The JAR file does not exist.
	 */
	private File getJarFile(String jarFilePath) {
		File jarFile = new File(jarFilePath);
		return jarFile;
	}

	// --------------------------------------------------------------------------------------------
	//  Logging and Exception Handling
	// --------------------------------------------------------------------------------------------

	/**
	 * Displays an exception message for incorrect command line arguments.
	 *
	 * @param e The exception to display.
	 * @return The return code for the process.
	 */
	private static int handleArgException(CliArgsException e) {

		System.out.println(e.getMessage());
		return 1;
	}

	/**
	 * Displays an optional exception message for incorrect program parametrization.
	 *
	 * @param e The exception to display.
	 * @return The return code for the process.
	 */
	private static int handleParametrizationException(ProgramParametrizationException e) {
		System.err.println(e.getMessage());
		return 1;
	}

	/**
	 * Displays a message for a program without a job to execute.
	 *
	 * @return The return code for the process.
	 */
	private static int handleMissingJobException() {
		System.err.println("The program didn't contain a Flink job. " +
			"Perhaps you forgot to call execute() on the execution environment.");
		return 1;
	}

	/**
	 * Displays an exception message.
	 *
	 * @param t The exception to display.
	 * @return The return code for the process.
	 */
	private static int handleError(Throwable t) {

		if (t.getCause() instanceof InvalidProgramException) {
			System.err.println(t.getCause().getMessage());
			StackTraceElement[] trace = t.getCause().getStackTrace();
			for (StackTraceElement ele: trace) {
				System.err.println("\t" + ele);
				if (ele.getMethodName().equals("main")) {
					break;
				}
			}
		}
		return 1;
	}

	private static void logAndSysout(String message) {
		System.out.println(message);
	}

	// --------------------------------------------------------------------------------------------
	//  Internal methods
	// --------------------------------------------------------------------------------------------

	private JobID parseJobId(String jobIdString) throws CliArgsException {

		final JobID jobId;
			jobId = JobID.fromHexString(jobIdString);
		return jobId;
	}

	/**
	 * Retrieves the {@link ClusterClient} from the given {@link CustomCommandLine} and runs the given
	 * {@link ClusterAction} against it.
	 *
	 * @param activeCommandLine to create the {@link ClusterDescriptor} from
	 * @param commandLine containing the parsed command line options
	 * @param clusterAction the cluster action to run against the retrieved {@link ClusterClient}.
	 * @param <ClusterID> type of the cluster id
	 * @throws FlinkException if something goes wrong
	 */
	private <ClusterID> void runClusterAction(CustomCommandLine activeCommandLine, CommandLine commandLine, ClusterAction<ClusterID> clusterAction) {
		Configuration executorConfig = activeCommandLine.applyCommandLineOptionsToConfiguration(commandLine);
		ClusterClientFactory<ClusterID> clusterClientFactory = clusterClientServiceLoader.getClusterClientFactory(executorConfig);

		ClusterID clusterId = clusterClientFactory.getClusterId(executorConfig);

		ClusterDescriptor<ClusterID> clusterDescriptor = clusterClientFactory.createClusterDescriptor(executorConfig);
		ClusterClient<ClusterID> clusterClient = clusterDescriptor.retrieve(clusterId).getClusterClient();
		clusterAction.runAction(clusterClient);
	}

	/**
	 * Internal interface to encapsulate cluster actions which are executed via
	 * the {@link ClusterClient}.
	 *
	 * @param <ClusterID> type of the cluster id
	 */
	@FunctionalInterface
	private interface ClusterAction<ClusterID> {

		/**
		 * Run the cluster action with the given {@link ClusterClient}.
		 *
		 * @param clusterClient to run the cluster action against
		 * @throws FlinkException if something goes wrong
		 */
		void runAction(ClusterClient<ClusterID> clusterClient) throws FlinkException;
	}

	// --------------------------------------------------------------------------------------------
	//  Entry point for executable
	// --------------------------------------------------------------------------------------------

	/**
	 * Parses the command line arguments and starts the requested action.
	 *
	 * @param args command line arguments of the client.
	 * @return The return code of the program
	 */
	public int parseParameters(String[] args) {

		// check for action
		if (args.length < 1) {
			CliFrontendParser.printHelp(customCommandLines);
			return 1;
		}

		// get action
		String action = args[0];

		// remove action from parameters
		final String[] params = Arrays.copyOfRange(args, 1, args.length);
		
		// do action
		switch (action) {
				case ACTION_RUN:
					run(params);
					return 0;
				case ACTION_RUN_APPLICATION:
					runApplication(params);
					return 0;
				case ACTION_LIST:
					list(params);
					return 0;
				case ACTION_INFO:
					info(params);
					return 0;
				case ACTION_CANCEL:
					cancel(params);
					return 0;
				case ACTION_STOP:
					stop(params);
					return 0;
				case ACTION_SAVEPOINT:
					savepoint(params);
					return 0;
				case "-h":
				case "--help":
					CliFrontendParser.printHelp(customCommandLines);
					return 0;
				case "-v":
				case "--version":
					String version = EnvironmentInformation.getVersion();
					String commitID = EnvironmentInformation.getRevisionInformation().commitId;
					System.out.print("Version: " + version);
					System.out.println(commitID.equals(EnvironmentInformation.UNKNOWN) ? "" : ", Commit ID: " + commitID);
					return 0;
				default:
					System.out.printf("\"%s\" is not a valid action.\n", action);
					System.out.println();
					System.out.println("Valid actions are \"run\", \"list\", \"info\", \"savepoint\", \"stop\", or \"cancel\".");
					System.out.println();
					System.out.println("Specify the version option (-v or --version) to print Flink version.");
					System.out.println();
					System.out.println("Specify the help option (-h or --help) to get help on the command.");
					return 1;
			}
	}

	/**
	 * Submits the job based on the arguments.
	 */
	public static void main(final String[] args) {

		// 1. find the configuration directory
		String configurationDirectory = getConfigurationDirectoryFromEnv();

		// 2. load the global configuration
		Configuration configuration = GlobalConfiguration.loadConfiguration(configurationDirectory);

		// 3. load the custom command lines
		List<CustomCommandLine> customCommandLines = loadCustomCommandLines(
			configuration,
			configurationDirectory);

		CliFrontend cli = new CliFrontend(configuration, customCommandLines);

		SecurityUtils.install(new SecurityConfiguration(cli.configuration));
		int retCode = SecurityUtils.getInstalledContext()
					.runSecured(() -> cli.parseParameters(args));
		System.exit(retCode);
		
		if (new File(CONFIG_DIRECTORY_FALLBACK_1).exists()) {
			location = CONFIG_DIRECTORY_FALLBACK_1;
		}
		else if (new File(CONFIG_DIRECTORY_FALLBACK_2).exists()) {
			location = CONFIG_DIRECTORY_FALLBACK_2;
		}

		return location;
	}

	/**
	 * Writes the given job manager address to the associated configuration object.
	 *
	 * @param address Address to write to the configuration
	 * @param config The configuration to write to
	 */
	static void setJobManagerAddressInConfig(Configuration config, InetSocketAddress address) {
		config.setString(JobManagerOptions.ADDRESS, address.getHostString());
		config.setInteger(JobManagerOptions.PORT, address.getPort());
		config.setString(RestOptions.ADDRESS, address.getHostString());
		config.setInteger(RestOptions.PORT, address.getPort());
	}

	public static List<CustomCommandLine> loadCustomCommandLines(Configuration configuration, String configurationDirectory) {
		List<CustomCommandLine> customCommandLines = new ArrayList<>();
		customCommandLines.add(new GenericCLI(configuration, configurationDirectory));

		//	Command line interface of the YARN session, with a special initialization here
		//	to prefix all options with y/yarn.
		final String flinkYarnSessionCLI = "org.apache.flink.yarn.cli.FlinkYarnSessionCli";
			customCommandLines.add(
				loadCustomCommandLine(flinkYarnSessionCLI,
					configuration,
					configurationDirectory,
					"y",
					"yarn"));

		//	Tips: DefaultCLI must be added at last, because getActiveCustomCommandLine(..) will get the
		//	      active CustomCommandLine in order and DefaultCLI isActive always return true.
		customCommandLines.add(new DefaultCLI(configuration));

		return customCommandLines;
	}

	public CustomCommandLine validateAndGetActiveCommandLine(CommandLine commandLine) {
		for (CustomCommandLine cli : customCommandLines) {
			LOG.debug("Checking custom commandline {}, isActive: {}", cli, cli.isActive(commandLine));
			if (cli.isActive(commandLine)) {
				return cli;
			}
		}
		throw new IllegalStateException("No valid command-line found.");
	}


	private static CustomCommandLine loadCustomCommandLine(String className, Object... params)  {

		Class<? extends CustomCommandLine> customCliClass =
			Class.forName(className).asSubclass(CustomCommandLine.class);

		// construct class types from the parameters
		Class<?>[] types = new Class<?>[params.length];
		for (int i = 0; i < params.length; i++) {
			checkNotNull(params[i], "Parameters for custom command-lines may not be null.");
			types[i] = params[i].getClass();
		}

		Constructor<? extends CustomCommandLine> constructor = customCliClass.getConstructor(types);

		return constructor.newInstance(params);
	}

}
