package org.apache.flink.python;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.core.memory.ByteArrayOutputStreamWithPos;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.python.env.PythonEnvironmentManager;
import org.apache.flink.python.metric.FlinkMetricContainer;
import org.apache.flink.util.Preconditions;

import org.apache.beam.model.fnexecution.v1.BeamFnApi;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.core.construction.PipelineOptionsTranslation;
import org.apache.beam.runners.core.construction.graph.ExecutableStage;
import org.apache.beam.runners.fnexecution.control.BundleProgressHandler;
import org.apache.beam.runners.fnexecution.control.DefaultJobBundleFactory;
import org.apache.beam.runners.fnexecution.control.JobBundleFactory;
import org.apache.beam.runners.fnexecution.control.OutputReceiverFactory;
import org.apache.beam.runners.fnexecution.control.RemoteBundle;
import org.apache.beam.runners.fnexecution.control.StageBundleFactory;
import org.apache.beam.runners.fnexecution.provisioning.JobInfo;
import org.apache.beam.runners.fnexecution.state.StateRequestHandler;
import org.apache.beam.sdk.fn.data.FnDataReceiver;
import org.apache.beam.sdk.options.ExperimentalOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.PortablePipelineOptions;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.vendor.grpc.v1p21p0.com.google.protobuf.Struct;

import javax.annotation.Nullable;

import java.util.Map;

/**
 * An base class for {@link PythonFunctionRunner}.
 *
 * @param <IN> Type of the input elements.
 */
@Internal
public abstract class AbstractPythonFunctionRunner<IN> implements PythonFunctionRunner<IN> {

	private static final String MAIN_INPUT_ID = "input";

	private final String taskName;

	/**
	 * The Python function execution result receiver.
	 */
	protected final FnDataReceiver<byte[]> resultReceiver;

	/**
	 * The Python execution environment manager.
	 */
	private final PythonEnvironmentManager environmentManager;

	/**
	 * The options used to configure the Python worker process.
	 */
	private final Map<String, String> jobOptions;

	/**
	 * The bundle factory which has all job-scoped information and can be used to create a {@link StageBundleFactory}.
	 */
	private transient JobBundleFactory jobBundleFactory;

	/**
	 * The bundle factory which has all of the resources it needs to provide new {@link RemoteBundle}.
	 */
	private transient StageBundleFactory stageBundleFactory;

	/**
	 * Handler for state requests.
	 */
	private final StateRequestHandler stateRequestHandler;

	/**
	 * Handler for bundle progress messages, both during bundle execution and on its completion.
	 */
	private transient BundleProgressHandler progressHandler;

	/**
	 * A bundle handler for handling input elements by forwarding them to a remote environment for processing.
	 * It holds a collection of {@link FnDataReceiver}s which actually perform the data forwarding work.
	 *
	 * <p>When a RemoteBundle is closed, it will block until bundle processing is finished on remote resources,
	 * and throw an exception if bundle processing has failed.
	 */
	private transient RemoteBundle remoteBundle;

	/**
	 * The receiver which forwards the input elements to a remote environment for processing.
	 */
	protected transient FnDataReceiver<WindowedValue<byte[]>> mainInputReceiver;

	/**
	 * Reusable OutputStream used to holding the serialized input elements.
	 */
	protected transient ByteArrayOutputStreamWithPos baos;

	/**
	 * OutputStream Wrapper.
	 */
	protected transient DataOutputViewStreamWrapper baosWrapper;

	/**
	 * The flinkMetricContainer will be set to null if metric is configured to be turned off.
	 */
	@Nullable protected FlinkMetricContainer flinkMetricContainer;

	public AbstractPythonFunctionRunner(
		String taskName,
		FnDataReceiver<byte[]> resultReceiver,
		PythonEnvironmentManager environmentManager,
		StateRequestHandler stateRequestHandler,
		Map<String, String> jobOptions,
		@Nullable FlinkMetricContainer flinkMetricContainer) {
		this.taskName = Preconditions.checkNotNull(taskName);
		this.resultReceiver = Preconditions.checkNotNull(resultReceiver);
		this.environmentManager = Preconditions.checkNotNull(environmentManager);
		this.stateRequestHandler = Preconditions.checkNotNull(stateRequestHandler);
		this.jobOptions = Preconditions.checkNotNull(jobOptions);
		this.flinkMetricContainer = flinkMetricContainer;
	}

	@Override
	public void open() throws Exception {
		baos = new ByteArrayOutputStreamWithPos();
		baosWrapper = new DataOutputViewStreamWrapper(baos);

		// The creation of stageBundleFactory depends on the initialized environment manager.
		environmentManager.open();

		PortablePipelineOptions portableOptions =
			PipelineOptionsFactory.as(PortablePipelineOptions.class);
		// one operator has one Python SDK harness
		portableOptions.setSdkWorkerParallelism(1);
		ExperimentalOptions experimentalOptions = portableOptions.as(ExperimentalOptions.class);
		for (Map.Entry<String, String> entry : jobOptions.entrySet()) {
			ExperimentalOptions.addExperiment(experimentalOptions,
				String.join("=", entry.getKey(), entry.getValue()));
		}

		Struct pipelineOptions = PipelineOptionsTranslation.toProto(portableOptions);

		jobBundleFactory = createJobBundleFactory(pipelineOptions);
		stageBundleFactory = createStageBundleFactory();
		progressHandler = getProgressHandler(flinkMetricContainer);
	}

	/**
	 * Ignore bundle progress if flinkMetricContainer is null. The flinkMetricContainer will be set
	 * to null if metric is configured to be turned off.
	 */
	private BundleProgressHandler getProgressHandler(FlinkMetricContainer flinkMetricContainer) {
		if (flinkMetricContainer == null) {
			return BundleProgressHandler.ignored();
		} else {
			return new BundleProgressHandler() {
				@Override
				public void onProgress(BeamFnApi.ProcessBundleProgressResponse progress) {
					flinkMetricContainer.updateMetrics(taskName, progress.getMonitoringInfosList());
				}

				@Override
				public void onCompleted(BeamFnApi.ProcessBundleResponse response) {
					flinkMetricContainer.updateMetrics(taskName, response.getMonitoringInfosList());
				}
			};
		}
	}

	/**
	 * To make the error messages more user friendly, throws an exception with the boot logs.
	 */
	private StageBundleFactory createStageBundleFactory() throws Exception {
			return jobBundleFactory.forStage(createExecutableStage());
		
	}

	@Override
	public void close() throws Exception {
		jobBundleFactory.close();
		environmentManager.close();
	}

	@Override
	public void startBundle() {
		
		remoteBundle = stageBundleFactory.getBundle(
							createOutputReceiverFactory(), 
							stateRequestHandler, 
							progressHandler);
		mainInputReceiver = remoteBundle.getInputReceivers().get(MAIN_INPUT_ID);
	}

	@Override
	public void finishBundle()  {
		remoteBundle.close();
	}

	@VisibleForTesting
	public JobBundleFactory createJobBundleFactory(Struct pipelineOptions) {
		return DefaultJobBundleFactory.create(
			JobInfo.create(taskName, taskName, environmentManager.createRetrievalToken(), pipelineOptions));
	}

	/**
	 * Creates a specification which specifies the portability Python execution environment.
	 * It's used by Beam's portability framework to creates the actual Python execution environment.
	 */
	protected RunnerApi.Environment createPythonExecutionEnvironment() throws Exception {
		return environmentManager.createEnvironment();
	}

	/**
	 * Creates a {@link ExecutableStage} which contains the Python user-defined functions to be executed
	 * and all the other information needed to execute them, such as the execution environment, the input
	 * and output coder, etc.
	 */
	public abstract ExecutableStage createExecutableStage() throws Exception;

	public abstract OutputReceiverFactory createOutputReceiverFactory();
}
