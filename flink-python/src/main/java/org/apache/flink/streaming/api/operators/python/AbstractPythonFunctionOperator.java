package org.apache.flink.streaming.api.operators.python;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.python.PythonConfig;
import org.apache.flink.python.PythonFunctionRunner;
import org.apache.flink.python.PythonOptions;
import org.apache.flink.python.env.ProcessPythonEnvironmentManager;
import org.apache.flink.python.env.PythonDependencyInfo;
import org.apache.flink.python.env.PythonEnvironmentManager;
import org.apache.flink.python.metric.FlinkMetricContainer;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.memory.MemoryReservationException;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.functions.python.PythonEnv;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.flink.streaming.api.utils.ClassLeakCleaner.cleanUpLeakingClasses;

/**
 * Base class for all stream operators to execute Python functions.
 */
@Internal
public abstract class AbstractPythonFunctionOperator<IN, OUT>
		extends AbstractStreamOperator<OUT>
		implements OneInputStreamOperator<IN, OUT>, BoundedOneInput {

	/**
	 * The {@link PythonFunctionRunner} which is responsible for Python user-defined function execution.
	 */
	private transient PythonFunctionRunner<IN> pythonFunctionRunner;

	/**
	 * Use an AtomicBoolean because we start/stop bundles by a timer thread.
	 */
	private transient AtomicBoolean bundleStarted;

	/**
	 * Number of processed elements in the current bundle.
	 */
	private transient int elementCount;

	/**
	 * Max number of elements to include in a bundle.
	 */
	private transient int maxBundleSize;

	/**
	 * Max duration of a bundle.
	 */
	private transient long maxBundleTimeMills;

	/**
	 * Time that the last bundle was finished.
	 */
	private transient long lastFinishBundleTime;

	/**
	 * A timer that finishes the current bundle after a fixed amount of time.
	 */
	private transient ScheduledFuture<?> checkFinishBundleTimer;

	/**
	 * Callback to be executed after the current bundle was finished.
	 */
	private transient Runnable bundleFinishedCallback;

	/**
	 * The size of the reserved memory from the MemoryManager.
	 */
	private transient long reservedMemory;

	/**
	 * The python config.
	 */
	private final PythonConfig config;

	public AbstractPythonFunctionOperator(Configuration config) {
		this.config = new PythonConfig(Preconditions.checkNotNull(config));
		this.chainingStrategy = ChainingStrategy.ALWAYS;
	}

	public PythonConfig getPythonConfig() {
		return config;
	}

	@Override
	public void open() throws Exception {
		try {
			this.bundleStarted = new AtomicBoolean(false);

			if (config.isUsingManagedMemory()) {
				reserveMemoryForPythonWorker();
			}

			this.maxBundleSize = config.getMaxBundleSize();
			if (this.maxBundleSize <= 0) {
				this.maxBundleSize = PythonOptions.MAX_BUNDLE_SIZE.defaultValue();
			}

			this.maxBundleTimeMills = config.getMaxBundleTimeMills();
			if (this.maxBundleTimeMills <= 0L) {
				this.maxBundleTimeMills = PythonOptions.MAX_BUNDLE_TIME_MILLS.defaultValue();
			}

			this.pythonFunctionRunner = createPythonFunctionRunner();
			this.pythonFunctionRunner.open();

			this.elementCount = 0;
			this.lastFinishBundleTime = getProcessingTimeService().getCurrentProcessingTime();

			// Schedule timer to check timeout of finish bundle.
			long bundleCheckPeriod = Math.max(this.maxBundleTimeMills, 1);
			this.checkFinishBundleTimer =
				getProcessingTimeService()
					.scheduleAtFixedRate(
						// ProcessingTimeService callbacks are executed under the checkpointing lock
						timestamp -> checkInvokeFinishBundleByTime(), bundleCheckPeriod, bundleCheckPeriod);
		} finally {
			super.open();
		}
	}

	@Override
	public void close() throws Exception {
		try {
			invokeFinishBundle();
		} finally {
			super.close();
			cleanUpLeakingClasses(this.getClass().getClassLoader());
		}
	}

	@Override
	public void dispose() throws Exception {
				checkFinishBundleTimer.cancel(true);
				pythonFunctionRunner.close();
				getContainingTask().getEnvironment().getMemoryManager().releaseMemory(this, reservedMemory);
			super.dispose();
	}

	@Override
	public void endInput() throws Exception {
		invokeFinishBundle();
	}

	@Override
	public void processElement(StreamRecord<IN> element) throws Exception {
		checkInvokeStartBundle();
		pythonFunctionRunner.processElement(element.getValue());
		checkInvokeFinishBundleByCount();
	}

	@Override
	public void prepareSnapshotPreBarrier(long checkpointId) throws Exception {
		try {
			// Ensures that no new bundle gets started
			invokeFinishBundle();
			super.prepareSnapshotPreBarrier(checkpointId);
		}
	}

	@Override
	public void processWatermark(Watermark mark) throws Exception {
		// Due to the asynchronous communication with the SDK harness,
		// a bundle might still be in progress and not all items have
		// yet been received from the SDK harness. If we just set this
		// watermark as the new output watermark, we could violate the
		// order of the records, i.e. pending items in the SDK harness
		// could become "late" although they were "on time".
		//
		// We can solve this problem using one of the following options:
		//
		// 1) Finish the current bundle and emit this watermark as the
		//    new output watermark. Finishing the bundle ensures that
		//    all the items have been processed by the SDK harness and
		//    the execution results sent to the downstream operator.
		//
		// 2) Hold on the output watermark for as long as the current
		//    bundle has not been finished. We have to remember to manually
		//    finish the bundle in case we receive the final watermark.
		//    To avoid latency, we should process this watermark again as
		//    soon as the current bundle is finished.
		//
		// Approach 1) is the easiest and gives better latency, yet 2)
		// gives better throughput due to the bundle not getting cut on
		// every watermark. So we have implemented 2) below.
		if (mark.getTimestamp() == Long.MAX_VALUE) {
			invokeFinishBundle();
			super.processWatermark(mark);
		} else if (!bundleStarted.get()) {
			// forward the watermark immediately if the bundle is already finished.
			super.processWatermark(mark);
		} else {
			// It is not safe to advance the output watermark yet, so add a hold on the current
			// output watermark.
			bundleFinishedCallback = { () -> super.processWatermark(mark); }
		}
	}

	/**
	 * Creates the {@link PythonFunctionRunner} which is responsible for Python user-defined function execution.
	 */
	public abstract PythonFunctionRunner<IN> createPythonFunctionRunner() throws Exception;

	/**
	 * Returns the {@link PythonEnv} used to create PythonEnvironmentManager..
	 */
	public abstract PythonEnv getPythonEnv();

	/**
	 * Sends the execution results to the downstream operator.
	 */
	public abstract void emitResults();

	/**
	 * Reserves the memory used by the Python worker from the MemoryManager. This makes sure that
	 * the memory used by the Python worker is managed by Flink.
	 */
	private void reserveMemoryForPythonWorker() throws MemoryReservationException {
		long requiredPythonWorkerMemory = MemorySize.parse(config.getPythonFrameworkMemorySize())
			.add(MemorySize.parse(config.getPythonDataBufferMemorySize()))
			.getBytes();
		MemoryManager memoryManager = getContainingTask().getEnvironment().getMemoryManager();
		long availableManagedMemory = memoryManager.computeMemorySize(
			getOperatorConfig().getManagedMemoryFraction());
		if (requiredPythonWorkerMemory <= availableManagedMemory) {
			memoryManager.reserveMemory(this, requiredPythonWorkerMemory);
			this.reservedMemory = requiredPythonWorkerMemory;
			// TODO enforce the memory limit of the Python worker
		} else {
			this.reservedMemory = -1;
		}
	}

	/**
	 * Checks whether to invoke startBundle.
	 */
	private void checkInvokeStartBundle() throws Exception {
		if (bundleStarted.compareAndSet(false, true)) {
			pythonFunctionRunner.startBundle();
		}
	}

	/**
	 * Checks whether to invoke finishBundle by elements count. Called in processElement.
	 */
	private void checkInvokeFinishBundleByCount() throws Exception {
		elementCount++;
		if (elementCount >= maxBundleSize) {
			invokeFinishBundle();
		}
	}

	/**
	 * Checks whether to invoke finishBundle by timeout.
	 */
	private void checkInvokeFinishBundleByTime() throws Exception {
		long now = getProcessingTimeService().getCurrentProcessingTime();
		if (now - lastFinishBundleTime >= maxBundleTimeMills) {
			invokeFinishBundle();
		}
	}

	private void invokeFinishBundle() throws Exception {
		if (bundleStarted.compareAndSet(true, false)) {
			pythonFunctionRunner.finishBundle();

			emitResults();
			elementCount = 0;
			lastFinishBundleTime = getProcessingTimeService().getCurrentProcessingTime();
			// callback only after current bundle was fully finalized
			if (bundleFinishedCallback != null) {
				bundleFinishedCallback.run();
				bundleFinishedCallback = null;
			}
		}
	}

	protected PythonEnvironmentManager createPythonEnvironmentManager() {
		PythonDependencyInfo dependencyInfo = PythonDependencyInfo.create(
			config, getRuntimeContext().getDistributedCache());

		PythonEnv pythonEnv = getPythonEnv();
		
		if (pythonEnv.getExecType() == PythonEnv.ExecType.PROCESS) {
			return new ProcessPythonEnvironmentManager(
				dependencyInfo,
				getContainingTask().getEnvironment().getTaskManagerInfo().getTmpDirectories(),
				System.getenv());
		} 
	}

	protected FlinkMetricContainer getFlinkMetricContainer() {
		return this.config.isMetricEnabled() ?
			new FlinkMetricContainer(getRuntimeContext().getMetricGroup()) : null;
	}
}
