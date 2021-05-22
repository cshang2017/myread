package org.apache.flink.runtime.operators.chaining;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.api.common.functions.Function;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.metrics.Counter;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.metrics.groups.OperatorIOMetricGroup;
import org.apache.flink.runtime.metrics.groups.OperatorMetricGroup;
import org.apache.flink.runtime.operators.BatchTask;
import org.apache.flink.runtime.operators.util.DistributedRuntimeUDFContext;
import org.apache.flink.runtime.operators.util.TaskConfig;
import org.apache.flink.runtime.operators.util.metrics.CountingCollector;
import org.apache.flink.util.Collector;

import java.util.Map;

/**
 * The interface to be implemented by drivers that do not run in an own task context, but are chained to other
 * tasks.
 */
public abstract class ChainedDriver<IT, OT> implements Collector<IT> {

	protected TaskConfig config;

	protected String taskName;

	protected Collector<OT> outputCollector;
	
	protected ClassLoader userCodeClassLoader;
	
	private DistributedRuntimeUDFContext udfContext;

	protected ExecutionConfig executionConfig;

	protected boolean objectReuseEnabled = false;
	
	protected OperatorMetricGroup metrics;
	
	protected Counter numRecordsIn;
	
	protected Counter numRecordsOut;

	
	public void setup(TaskConfig config, String taskName, Collector<OT> outputCollector,
			AbstractInvokable parent, ClassLoader userCodeClassLoader, ExecutionConfig executionConfig,
			Map<String, Accumulator<?,?>> accumulatorMap)
	{
		this.config = config;
		this.taskName = taskName;
		this.userCodeClassLoader = userCodeClassLoader;
		this.metrics = parent.getEnvironment().getMetricGroup().getOrAddOperator(taskName);
		this.numRecordsIn = this.metrics.getIOMetricGroup().getNumRecordsInCounter();
		this.numRecordsOut = this.metrics.getIOMetricGroup().getNumRecordsOutCounter();
		this.outputCollector = new CountingCollector<>(outputCollector, numRecordsOut);

		Environment env = parent.getEnvironment();

		if (parent instanceof BatchTask) {
			this.udfContext = ((BatchTask<?, ?>) parent).createRuntimeContext(metrics);
		} else {
			this.udfContext = new DistributedRuntimeUDFContext(env.getTaskInfo(), userCodeClassLoader,
					parent.getExecutionConfig(), env.getDistributedCacheEntries(), accumulatorMap, metrics, env.getExternalResourceInfoProvider()
			);
		}

		this.executionConfig = executionConfig;
		this.objectReuseEnabled = executionConfig.isObjectReuseEnabled();

		setup(parent);
	}

	public abstract void setup(AbstractInvokable parent);

	public abstract void openTask() throws Exception;

	public abstract void closeTask() throws Exception;

	public abstract void cancelTask();

	public abstract Function getStub();

	public abstract String getTaskName();

	@Override
	public abstract void collect(IT record);

	public OperatorIOMetricGroup getIOMetrics() {
		return this.metrics.getIOMetricGroup();
	}
	
	protected RuntimeContext getUdfRuntimeContext() {
		return this.udfContext;
	}

	@SuppressWarnings("unchecked")
	public void setOutputCollector(Collector<?> outputCollector) {
		this.outputCollector = new CountingCollector<>((Collector<OT>) outputCollector, numRecordsOut);
	}

	public Collector<OT> getOutputCollector() {
		return outputCollector;
	}
	
	public TaskConfig getTaskConfig() {
		return this.config;
	}
}
