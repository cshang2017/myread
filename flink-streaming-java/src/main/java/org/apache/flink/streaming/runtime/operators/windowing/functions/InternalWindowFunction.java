package org.apache.flink.streaming.runtime.operators.windowing.functions;

import org.apache.flink.api.common.functions.Function;
import org.apache.flink.api.common.state.KeyedStateStore;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * Internal interface for functions that are evaluated over keyed (grouped) windows.
 *
 * @param <IN> The type of the input value.
 * @param <OUT> The type of the output value.
 * @param <KEY> The type of the key.
 */
public interface InternalWindowFunction<IN, OUT, KEY, W extends Window> extends Function {
	/**
	 * Evaluates the window and outputs none or several elements.
	 *
	 * @param context The context in which the window is being evaluated.
	 * @param input The elements in the window being evaluated.
	 * @param out A collector for emitting elements.
	 *
	 * @throws Exception The function may throw exceptions to fail the program and trigger recovery.
	 */
	void process(KEY key, W window, InternalWindowContext context, IN input, Collector<OUT> out) throws Exception;

	/**
	 * Deletes any state in the {@code Context} when the Window expires
	 * (the watermark passes its {@code maxTimestamp} + {@code allowedLateness}).
	 *
	 * @param context The context to which the window is being evaluated
	 * @throws Exception The function may throw exceptions to fail the program and trigger recovery.
	 */
	void clear(W window, InternalWindowContext context) throws Exception;

	/**
	 * A context for {@link InternalWindowFunction}, similar to
	 * {@link org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction.Context} but
	 * for internal use.
	 */
	interface InternalWindowContext extends java.io.Serializable {
		long currentProcessingTime();

		long currentWatermark();

		KeyedStateStore windowState();

		KeyedStateStore globalState();

		<X> void output(OutputTag<X> outputTag, X value);
	}
}
