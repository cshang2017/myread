package org.apache.flink.runtime.operators;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.io.CleanupWhenUnsuccessful;
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.api.common.io.RichOutputFormat;
import org.apache.flink.api.common.typeutils.TypeComparatorFactory;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerFactory;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.IOReadableWritable;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.runtime.execution.CancelTaskException;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.io.network.api.reader.MutableReader;
import org.apache.flink.runtime.io.network.api.reader.MutableRecordReader;
import org.apache.flink.runtime.io.network.partition.consumer.UnionInputGate;
import org.apache.flink.runtime.jobgraph.InputOutputFormatContainer;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.metrics.groups.OperatorIOMetricGroup;
import org.apache.flink.runtime.metrics.groups.OperatorMetricGroup;
import org.apache.flink.runtime.operators.chaining.ExceptionInChainedStubException;
import org.apache.flink.runtime.operators.sort.UnilateralSortMerger;
import org.apache.flink.runtime.operators.util.CloseableInputProvider;
import org.apache.flink.runtime.operators.util.DistributedRuntimeUDFContext;
import org.apache.flink.runtime.operators.util.ReaderIterator;
import org.apache.flink.runtime.operators.util.TaskConfig;
import org.apache.flink.runtime.plugable.DeserializationDelegate;
import org.apache.flink.util.MutableObjectIterator;

import org.apache.commons.lang3.tuple.Pair;

/**
 * DataSinkTask which is executed by a task manager. The task hands the data to an output format.
 * 
 * @see OutputFormat
 */
public class DataSinkTask<IT> extends AbstractInvokable {

	// OutputFormat instance. volatile, because the asynchronous canceller may access it
	private volatile OutputFormat<IT> format;

	private MutableReader<?> inputReader;

	// input reader
	private MutableObjectIterator<IT> reader;

	// The serializer for the input type
	private TypeSerializerFactory<IT> inputTypeSerializerFactory;
	
	// local strategy
	private CloseableInputProvider<IT> localStrategy;

	// task configuration
	private TaskConfig config;
	
	// cancel flag
	private volatile boolean taskCanceled;
	
	private volatile boolean cleanupCalled;

	/**
	 * Create an Invokable task and set its environment.
	 *
	 * @param environment The environment assigned to this invokable.
	 */
	public DataSinkTask(Environment environment) {
		super(environment);
	}

	@Override
	public void invoke() throws Exception {
		initOutputFormat();
		initInputReaders();

		RuntimeContext ctx = createRuntimeContext();

		final Counter numRecordsIn;
		{
			Counter tmpNumRecordsIn;
				OperatorIOMetricGroup ioMetricGroup = ((OperatorMetricGroup) ctx.getMetricGroup()).getIOMetricGroup();
				ioMetricGroup.reuseInputMetricsForTask();
				ioMetricGroup.reuseOutputMetricsForTask();
				tmpNumRecordsIn = ioMetricGroup.getNumRecordsInCounter();
			numRecordsIn = tmpNumRecordsIn;
		}

		if(RichOutputFormat.class.isAssignableFrom(this.format.getClass())){
			((RichOutputFormat) this.format).setRuntimeContext(ctx);
		}

		ExecutionConfig executionConfig = getExecutionConfig();

		boolean objectReuseEnabled = executionConfig.isObjectReuseEnabled();
		
		try {
			// initialize local strategies
			MutableObjectIterator<IT> input1;
			switch (this.config.getInputLocalStrategy(0)) {
			case NONE:
				// nothing to do
				localStrategy = null;
				input1 = reader;
				break;
			case SORT:
				// initialize sort local strategy
				try {
					// get type comparator
					TypeComparatorFactory<IT> compFact = this.config.getInputComparator(0,
							getUserCodeClassLoader());
					
					// initialize sorter
					UnilateralSortMerger<IT> sorter = new UnilateralSortMerger<IT>(
							getEnvironment().getMemoryManager(), 
							getEnvironment().getIOManager(),
							this.reader, this, this.inputTypeSerializerFactory, compFact.createComparator(),
							this.config.getRelativeMemoryInput(0), this.config.getFilehandlesInput(0),
							this.config.getSpillingThresholdInput(0),
							this.config.getUseLargeRecordHandler(),
							this.getExecutionConfig().isObjectReuseEnabled());
					
					this.localStrategy = sorter;
					input1 = sorter.getIterator();
				break;
			default:
				throw new RuntimeException("Invalid local strategy for DataSinkTask");
			}
			
			// read the reader and write it to the output
			
			final TypeSerializer<IT> serializer = this.inputTypeSerializerFactory.getSerializer();
			final MutableObjectIterator<IT> input = input1;
			final OutputFormat<IT> format = this.format;


			// check if task has been canceled
			if (this.taskCanceled) {
				return;
			}

			// open
			format.open(this.getEnvironment().getTaskInfo().getIndexOfThisSubtask(), this.getEnvironment().getTaskInfo().getNumberOfParallelSubtasks());

			if (objectReuseEnabled) {
				IT record = serializer.createInstance();

				// work!
				while (!this.taskCanceled && ((record = input.next(record)) != null)) {
					numRecordsIn.inc();
					format.writeRecord(record);
				}
			} else {
				IT record;

				// work!
				while (!this.taskCanceled && ((record = input.next()) != null)) {
					numRecordsIn.inc();
					format.writeRecord(record);
				}
			}
			
			// close. We close here such that a regular close throwing an exception marks a task as failed.
			if (!this.taskCanceled) {
				this.format.close();
				this.format = null;
			}
		}
		finally {
			if (this.format != null) {
				// close format, if it has not been closed, yet.
				// This should only be the case if we had a previous error, or were canceled.
					this.format.close();
			}
			// close local strategy if necessary
			if (localStrategy != null) {
					this.localStrategy.close();
			}

			BatchTask.clearReaders(new MutableReader<?>[]{inputReader});
		}

	}

	@Override
	public void cancel() throws Exception {
		this.taskCanceled = true;
		OutputFormat<IT> format = this.format;
		if (format != null) {
				this.format.close();
			
			// make a best effort to clean up
				if (!cleanupCalled && format instanceof CleanupWhenUnsuccessful) {
					cleanupCalled = true;
					((CleanupWhenUnsuccessful) format).tryCleanupOnError();
				}
		}
		
	}

	/**
	 * Initializes the OutputFormat implementation and configuration.
	 * 
	 * @throws RuntimeException
	 *         Throws if instance of OutputFormat implementation can not be
	 *         obtained.
	 */
	private void initOutputFormat() {
		ClassLoader userCodeClassLoader = getUserCodeClassLoader();
		// obtain task configuration (including stub parameters)
		Configuration taskConf = getTaskConfiguration();
		this.config = new TaskConfig(taskConf);

		final Pair<OperatorID, OutputFormat<IT>> operatorIDAndOutputFormat;
		InputOutputFormatContainer formatContainer = new InputOutputFormatContainer(config, userCodeClassLoader);
			operatorIDAndOutputFormat = formatContainer.getUniqueOutputFormat();
			this.format = operatorIDAndOutputFormat.getValue();

			// check if the class is a subclass, if the check is required
			if (!OutputFormat.class.isAssignableFrom(this.format.getClass())) {
				throw new RuntimeException("The class '" + this.format.getClass().getName() + "' is not a subclass of '" + 
						OutputFormat.class.getName() + "' as is required.");
			}

		Thread thread = Thread.currentThread();
		ClassLoader original = thread.getContextClassLoader();
		// configure the stub. catch exceptions here extra, to report them as originating from the user code 
			thread.setContextClassLoader(userCodeClassLoader);
			this.format.configure(formatContainer.getParameters(operatorIDAndOutputFormat.getKey()));
			thread.setContextClassLoader(original);
	}

	/**
	 * Initializes the input readers of the DataSinkTask.
	 * 
	 * @throws RuntimeException
	 *         Thrown in case of invalid task input configuration.
	 */
	@SuppressWarnings("unchecked")
	private void initInputReaders() throws Exception {
		int numGates = 0;
		//  ---------------- create the input readers ---------------------
		// in case where a logical input unions multiple physical inputs, create a union reader
		final int groupSize = this.config.getGroupSize(0);
		numGates += groupSize;
		if (groupSize == 1) {
			// non-union case
			inputReader = new MutableRecordReader<DeserializationDelegate<IT>>(
					getEnvironment().getInputGate(0),
					getEnvironment().getTaskManagerInfo().getTmpDirectories());
		} else if (groupSize > 1){
			// union case
			inputReader = new MutableRecordReader<IOReadableWritable>(
					new UnionInputGate(getEnvironment().getAllInputGates()),
					getEnvironment().getTaskManagerInfo().getTmpDirectories());
		} else {
			throw new Exception("Illegal input group size in task configuration: " + groupSize);
		}
		
		this.inputTypeSerializerFactory = this.config.getInputSerializer(0, getUserCodeClassLoader());
		@SuppressWarnings({ "rawtypes" })
		final MutableObjectIterator<?> iter = new ReaderIterator(inputReader, this.inputTypeSerializerFactory.getSerializer());
		this.reader = (MutableObjectIterator<IT>)iter;

		// final sanity check
		if (numGates != this.config.getNumInputs()) {
			throw new Exception("Illegal configuration: Number of input gates and group sizes are not consistent.");
		}
	}

	// ------------------------------------------------------------------------
	//                               Utilities
	// ------------------------------------------------------------------------
	
	/**
	 * Utility function that composes a string for logging purposes. The string includes the given message and
	 * the index of the task in its task group together with the number of tasks in the task group.
	 * 
	 * @param message The main message for the log.
	 * @return The string ready for logging.
	 */
	private String getLogString(String message) {
		return BatchTask.constructLogString(message, this.getEnvironment().getTaskInfo().getTaskName(), this);
	}

	public DistributedRuntimeUDFContext createRuntimeContext() {
		Environment env = getEnvironment();

		return new DistributedRuntimeUDFContext(env.getTaskInfo(), getUserCodeClassLoader(),
				getExecutionConfig(), env.getDistributedCacheEntries(), env.getAccumulatorRegistry().getUserMap(), 
				getEnvironment().getMetricGroup().getOrAddOperator(getEnvironment().getTaskInfo().getTaskName()), env.getExternalResourceInfoProvider());
	}
}
