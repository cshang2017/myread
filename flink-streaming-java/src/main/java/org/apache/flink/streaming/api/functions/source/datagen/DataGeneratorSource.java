package org.apache.flink.streaming.api.functions.source.datagen;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

/**
 * A data generator source that abstract data generator. It can be used to easy startup/test
 * for streaming job and performance testing.
 * It is stateful, re-scalable, possibly in parallel.
 */
@Experimental
public class DataGeneratorSource<T> extends RichParallelSourceFunction<T> implements CheckpointedFunction {

	private final DataGenerator<T> generator;
	private final long rowsPerSecond;

	transient volatile boolean isRunning;

	/**
	 * Creates a source that emits records by {@link DataGenerator} without controlling emit rate.
	 *
	 * @param generator data generator.
	 */
	public DataGeneratorSource(DataGenerator<T> generator) {
		this(generator, Long.MAX_VALUE);
	}

	/**
	 * Creates a source that emits records by {@link DataGenerator}.
	 *
	 * @param generator data generator.
	 * @param rowsPerSecond Control the emit rate.
	 */
	public DataGeneratorSource(DataGenerator<T> generator, long rowsPerSecond) {
		this.generator = generator;
		this.rowsPerSecond = rowsPerSecond;
	}

	@Override
	public void initializeState(FunctionInitializationContext context) throws Exception {
		this.generator.open("DataGenerator", context, getRuntimeContext());
		this.isRunning = true;
	}

	@Override
	public void snapshotState(FunctionSnapshotContext context) throws Exception {
		this.generator.snapshotState(context);
	}

	@Override
	public void run(SourceContext<T> ctx) throws Exception {
		double taskRowsPerSecond = (double) rowsPerSecond / getRuntimeContext().getNumberOfParallelSubtasks();
		long nextReadTime = System.currentTimeMillis();

		while (isRunning) {
			for (int i = 0; i < taskRowsPerSecond; i++) {
				if (isRunning && generator.hasNext()) {
					synchronized (ctx.getCheckpointLock()) {
						ctx.collect(this.generator.next());
					}
				} else {
					return;
				}
			}

			nextReadTime += 1000;
			long toWaitMs = nextReadTime - System.currentTimeMillis();
			while (toWaitMs > 0) {
				Thread.sleep(toWaitMs);
				toWaitMs = nextReadTime - System.currentTimeMillis();
			}
		}
	}

	@Override
	public void cancel() {
		isRunning = false;
	}
}
