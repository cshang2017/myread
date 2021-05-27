package org.apache.flink.runtime.operators;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.AbstractRichFunction;
import org.apache.flink.metrics.Counter;
import org.apache.flink.runtime.operators.util.metrics.CountingCollector;
import org.apache.flink.util.Collector;
import org.apache.flink.util.MutableObjectIterator;

/**
 * A driver that does nothing but forward data from its input to its output.
 * 
 * @param <T> The data type.
 */
public class NoOpDriver<T> implements Driver<AbstractRichFunction, T> {

	private TaskContext<AbstractRichFunction, T> taskContext;
	
	private volatile boolean running;

	private boolean objectReuseEnabled = false;

	@Override
	public void setup(TaskContext<AbstractRichFunction, T> context) {
		this.taskContext = context;
		this.running = true;
	}

	@Override
	public int getNumberOfInputs() {
		return 1;
	}
	
	@Override
	public Class<AbstractRichFunction> getStubType() {
		return null;
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
		final MutableObjectIterator<T> input = this.taskContext.getInput(0);
		final Collector<T> output = new CountingCollector<>(this.taskContext.getOutputCollector(), numRecordsOut);

		if (objectReuseEnabled) {
			T record = this.taskContext.<T>getInputSerializer(0).getSerializer().createInstance();

			while (this.running && ((record = input.next(record)) != null)) {
				output.collect(record);
			}
		} else {
			T record;
			while (this.running && ((record = input.next()) != null)) {
				output.collect(record);
			}

		}
	}
	
	@Override
	public void cleanup() {}

	@Override
	public void cancel() {
		this.running = false;
	}
}
