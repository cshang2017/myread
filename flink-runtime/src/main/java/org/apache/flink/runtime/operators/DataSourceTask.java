package org.apache.flink.runtime.operators;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.io.RichInputFormat;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerFactory;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.runtime.execution.CancelTaskException;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.io.network.api.writer.RecordWriter;
import org.apache.flink.runtime.jobgraph.InputOutputFormatContainer;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.jobgraph.tasks.InputSplitProvider;
import org.apache.flink.runtime.jobgraph.tasks.InputSplitProviderException;
import org.apache.flink.runtime.metrics.groups.OperatorIOMetricGroup;
import org.apache.flink.runtime.metrics.groups.OperatorMetricGroup;
import org.apache.flink.runtime.operators.chaining.ChainedDriver;
import org.apache.flink.runtime.operators.chaining.ExceptionInChainedStubException;
import org.apache.flink.runtime.operators.util.DistributedRuntimeUDFContext;
import org.apache.flink.runtime.operators.util.TaskConfig;
import org.apache.flink.runtime.operators.util.metrics.CountingCollector;
import org.apache.flink.util.Collector;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * DataSourceTask which is executed by a task manager. The task reads data and uses an 
 * {@link InputFormat} to create records from the input.
 * 
 * @see org.apache.flink.api.common.io.InputFormat
 */
public class DataSourceTask<OT> extends AbstractInvokable {

	private List<RecordWriter<?>> eventualOutputs;

	// Output collector
	private Collector<OT> output;

	// InputFormat instance
	private InputFormat<OT, InputSplit> format;

	// type serializer for the input
	private TypeSerializerFactory<OT> serializerFactory;
	
	// Task configuration
	private TaskConfig config;
	
	// tasks chained to this data source
	private ArrayList<ChainedDriver<?, ?>> chainedTasks;
	
	// cancel flag
	private volatile boolean taskCanceled = false;

	/**
	 * Create an Invokable task and set its environment.
	 *
	 * @param environment The environment assigned to this invokable.
	 */
	public DataSourceTask(Environment environment) {
		super(environment);
	}

	@Override
	public void invoke()   {
		// --------------------------------------------------------------------
		// Initialize
		// --------------------------------------------------------------------
		initInputFormat();

			initOutputs(getUserCodeClassLoader());

		// Invoke

		RuntimeContext ctx = createRuntimeContext();

		final Counter numRecordsOut;
		{
			Counter tmpNumRecordsOut;
				OperatorIOMetricGroup ioMetricGroup = ((OperatorMetricGroup) ctx.getMetricGroup()).getIOMetricGroup();
				ioMetricGroup.reuseInputMetricsForTask();
				if (this.config.getNumberOfChainedStubs() == 0) {
					ioMetricGroup.reuseOutputMetricsForTask();
				}
				tmpNumRecordsOut = ioMetricGroup.getNumRecordsOutCounter();
			numRecordsOut = tmpNumRecordsOut;
		}
		
		Counter completedSplitsCounter = ctx.getMetricGroup().counter("numSplitsProcessed");

		if (RichInputFormat.class.isAssignableFrom(this.format.getClass())) {
			((RichInputFormat) this.format).setRuntimeContext(ctx);
			((RichInputFormat) this.format).openInputFormat();
		}

		ExecutionConfig executionConfig = getExecutionConfig();

		boolean objectReuseEnabled = executionConfig.isObjectReuseEnabled();

		final TypeSerializer<OT> serializer = this.serializerFactory.getSerializer();
		
		try {
			// start all chained tasks
			BatchTask.openChainedTasks(this.chainedTasks, this);
			
			// get input splits to read
			final Iterator<InputSplit> splitIterator = getInputSplits();
			
			// for each assigned input split
			while (!this.taskCanceled && splitIterator.hasNext())
			{
				// get start and end
				final InputSplit split = splitIterator.next();

				final InputFormat<OT, InputSplit> format = this.format;
			
				// open input format
				format.open(split);
	
				try {
					final Collector<OT> output = new CountingCollector<>(this.output, numRecordsOut);

					if (objectReuseEnabled) {
						OT reuse = serializer.createInstance();

						// as long as there is data to read
						while (!this.taskCanceled && !format.reachedEnd()) {

							OT returned;
							if ((returned = format.nextRecord(reuse)) != null) {
								output.collect(returned);
							}
						}
					} else {
						// as long as there is data to read
						while (!this.taskCanceled && !format.reachedEnd()) {
							OT returned;
							if ((returned = format.nextRecord(serializer.createInstance())) != null) {
								output.collect(returned);
							}
						}
					}

				} finally {
					// close. We close here such that a regular close throwing an exception marks a task as failed.
					format.close();
				}
				completedSplitsCounter.inc();
			} // end for all input splits

			// close all chained tasks letting them report failure
			BatchTask.closeChainedTasks(this.chainedTasks, this);

			// close the output collector
			this.output.close();
		} finally {
			BatchTask.clearWriters(eventualOutputs);
			// --------------------------------------------------------------------
			// Closing
			// --------------------------------------------------------------------
			if (this.format != null && RichInputFormat.class.isAssignableFrom(this.format.getClass())) {
				((RichInputFormat) this.format).closeInputFormat();
			}
		}
	
	}

	@Override
	public void cancel() throws Exception {
		this.taskCanceled = true;
	}

	/**
	 * Initializes the InputFormat implementation and configuration.
	 * 
	 * @throws RuntimeException
	 *         Throws if instance of InputFormat implementation can not be
	 *         obtained.
	 */
	private void initInputFormat() {
		ClassLoader userCodeClassLoader = getUserCodeClassLoader();
		// obtain task configuration (including stub parameters)
		Configuration taskConf = getTaskConfiguration();
		this.config = new TaskConfig(taskConf);

		final Pair<OperatorID, InputFormat<OT, InputSplit>> operatorIdAndInputFormat;
		InputOutputFormatContainer formatContainer = new InputOutputFormatContainer(config, userCodeClassLoader);
		try {
			operatorIdAndInputFormat = formatContainer.getUniqueInputFormat();
			this.format = operatorIdAndInputFormat.getValue();

			// check if the class is a subclass, if the check is required
			if (!InputFormat.class.isAssignableFrom(this.format.getClass())) {
				throw new RuntimeException("The class '" + this.format.getClass().getName() + "' is not a subclass of '" +
						InputFormat.class.getName() + "' as is required.");
			}
		}

		Thread thread = Thread.currentThread();
		ClassLoader original = thread.getContextClassLoader();
		// configure the stub. catch exceptions here extra, to report them as originating from the user code
		try {
			thread.setContextClassLoader(userCodeClassLoader);
			this.format.configure(formatContainer.getParameters(operatorIdAndInputFormat.getKey()));
		}
		finally {
			thread.setContextClassLoader(original);
		}

		// get the factory for the type serializer
		this.serializerFactory = this.config.getOutputSerializer(userCodeClassLoader);
	}

	/**
	 * Creates a writer for each output. Creates an OutputCollector which forwards its input to all writers.
	 * The output collector applies the configured shipping strategy.
	 */
	private void initOutputs(ClassLoader cl) throws Exception {
		this.chainedTasks = new ArrayList<ChainedDriver<?, ?>>();
		this.eventualOutputs = new ArrayList<RecordWriter<?>>();

		this.output = BatchTask.initOutputs(this, cl, this.config, this.chainedTasks, this.eventualOutputs,
				getExecutionConfig(), getEnvironment().getAccumulatorRegistry().getUserMap());
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
		return getLogString(message, this.getEnvironment().getTaskInfo().getTaskName());
	}
	
	/**
	 * Utility function that composes a string for logging purposes. The string includes the given message and
	 * the index of the task in its task group together with the number of tasks in the task group.
	 *  
	 * @param message The main message for the log.
	 * @param taskName The name of the task.
	 * @return The string ready for logging.
	 */
	private String getLogString(String message, String taskName) {
		return BatchTask.constructLogString(message, taskName, this);
	}
	
	private Iterator<InputSplit> getInputSplits() {

		final InputSplitProvider provider = getEnvironment().getInputSplitProvider();

		return new Iterator<InputSplit>() {

			private InputSplit nextSplit;
			
			private boolean exhausted;

			@Override
			public boolean hasNext() {
				if (exhausted) {
					return false;
				}
				
				if (nextSplit != null) {
					return true;
				}

				final InputSplit split;
					split = provider.getNextInputSplit(getUserCodeClassLoader());

				if (split != null) {
					this.nextSplit = split;
					return true;
				}
				else {
					exhausted = true;
					return false;
				}
			}

			@Override
			public InputSplit next() {
				if (this.nextSplit == null && !hasNext()) {
					throw new NoSuchElementException();
				}

				final InputSplit tmp = this.nextSplit;
				this.nextSplit = null;
				return tmp;
			}

			@Override
			public void remove() {
				throw new UnsupportedOperationException();
			}
		};
	}

	public DistributedRuntimeUDFContext createRuntimeContext() {
		Environment env = getEnvironment();

		String sourceName =  getEnvironment().getTaskInfo().getTaskName().split("->")[0].trim();
		sourceName = sourceName.startsWith("CHAIN") ? sourceName.substring(6) : sourceName;

		return new DistributedRuntimeUDFContext(env.getTaskInfo(), getUserCodeClassLoader(),
				getExecutionConfig(), env.getDistributedCacheEntries(), env.getAccumulatorRegistry().getUserMap(), 
				getEnvironment().getMetricGroup().getOrAddOperator(sourceName), env.getExternalResourceInfoProvider());
	}
}
