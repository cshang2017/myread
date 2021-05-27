package org.apache.flink.runtime.operators;

import org.apache.flink.api.common.functions.Function;
import org.apache.flink.metrics.Counter;
import org.apache.flink.runtime.operators.util.metrics.CountingCollector;
import org.apache.flink.util.Collector;
import org.apache.flink.util.MutableObjectIterator;

public class UnionWithTempOperator<T> implements Driver<Function, T> {
	
	private static final int CACHED_INPUT = 0;
	private static final int STREAMED_INPUT = 1;
	
	private TaskContext<Function, T> taskContext;
	
	private volatile boolean running;
	
	
	@Override
	public void setup(TaskContext<Function, T> context) {
		this.taskContext = context;
		this.running = true;
	}

	@Override
	public int getNumberOfInputs() {
		return 2;
	}
	
	@Override
	public int getNumberOfDriverComparators() {
		return 0;
	}

	@Override
	public Class<Function> getStubType() {
		return null; // no UDF
	}

	@Override
	public void prepare() {}

	@Override
	public void run() throws Exception {
		
		final Collector<T> output = new CountingCollector<>(this.taskContext.getOutputCollector(), numRecordsOut);
		T reuse = this.taskContext.<T>getInputSerializer(STREAMED_INPUT).getSerializer().createInstance();
		T record;
		
		final MutableObjectIterator<T> input = this.taskContext.getInput(STREAMED_INPUT);
		while (this.running && ((record = input.next(reuse)) != null)) {
			output.collect(record);
		}
		
		final MutableObjectIterator<T> cache = this.taskContext.getInput(CACHED_INPUT);
		while (this.running && ((record = cache.next(reuse)) != null)) {
			output.collect(record);
		}
	}

	@Override
	public void cleanup() {}

	@Override
	public void cancel() {
		this.running = false;
	}
}
