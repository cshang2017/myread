package org.apache.flink.runtime.operators;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.metrics.Counter;
import org.apache.flink.runtime.operators.util.metrics.CountingCollector;
import org.apache.flink.runtime.operators.util.metrics.CountingMutableObjectIterator;
import org.apache.flink.runtime.util.NonReusingMutableToRegularIteratorWrapper;
import org.apache.flink.runtime.util.ReusingMutableToRegularIteratorWrapper;
import org.apache.flink.util.Collector;
import org.apache.flink.util.MutableObjectIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * MapPartition task which is executed by a Task Manager. The task has a single
 * input and one or multiple outputs. It is provided with a MapFunction
 * implementation.
 * <p>
 * The MapPartitionTask creates an iterator over all key-value pairs of its input and hands that to the <code>map_partition()</code> method
 * of the MapFunction.
 *
 * @see MapPartitionFunction
 *
 * @param <IT> The mapper's input data type.
 * @param <OT> The mapper's output data type.
 */
public class MapPartitionDriver<IT, OT> implements Driver<MapPartitionFunction<IT, OT>, OT> {

	private TaskContext<MapPartitionFunction<IT, OT>, OT> taskContext;

	private boolean objectReuseEnabled = false;

	@Override
	public void setup(TaskContext<MapPartitionFunction<IT, OT>, OT> context) {
		this.taskContext = context;
	}

	@Override
	public int getNumberOfInputs() {
		return 1;
	}

	@Override
	public Class<MapPartitionFunction<IT, OT>> getStubType() {
		@SuppressWarnings("unchecked")
		final Class<MapPartitionFunction<IT, OT>> clazz = (Class<MapPartitionFunction<IT, OT>>) (Class<?>) MapPartitionFunction.class;
		return clazz;
	}

	@Override
	public int getNumberOfDriverComparators() {
		return 0;
	}

	@Override
	public void prepare() {
		ExecutionConfig executionConfig = taskContext.getExecutionConfig();
		this.objectReuseEnabled = executionConfig.isObjectReuseEnabled();
	}

	@Override
	public void run() throws Exception {
		// cache references on the stack
		final MutableObjectIterator<IT> input = new CountingMutableObjectIterator<>(this.taskContext.<IT>getInput(0), numRecordsIn);
		final MapPartitionFunction<IT, OT> function = this.taskContext.getStub();
		final Collector<OT> output = new CountingCollector<>(this.taskContext.getOutputCollector(), numRecordsOut);

		if (objectReuseEnabled) {
			final ReusingMutableToRegularIteratorWrapper<IT> inIter = new ReusingMutableToRegularIteratorWrapper<IT>(input, this.taskContext.<IT>getInputSerializer(0).getSerializer());

			function.mapPartition(inIter, output);
		} else {
			final NonReusingMutableToRegularIteratorWrapper<IT> inIter = new NonReusingMutableToRegularIteratorWrapper<IT>(input, this.taskContext.<IT>getInputSerializer(0).getSerializer());

			function.mapPartition(inIter, output);
		}
	}

	@Override
	public void cleanup() {
		// mappers need no cleanup, since no strategies are used.
	}

	@Override
	public void cancel() {}
}
