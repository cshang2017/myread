package org.apache.flink.streaming.api.functions.windowing;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.functions.AbstractRichFunction;
import org.apache.flink.api.common.state.KeyedStateStore;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * Base abstract class for functions that are evaluated over keyed (grouped) windows using a context
 * for retrieving extra information.
 *
 * @param <IN> The type of the input value.
 * @param <OUT> The type of the output value.
 * @param <KEY> The type of the key.
 * @param <W> The type of {@code Window} that this window function can be applied on.
 */
@PublicEvolving
public abstract class ProcessWindowFunction<IN, OUT, KEY, W extends Window> extends AbstractRichFunction {

	private static final long serialVersionUID = 1L;

	/**
	 * Evaluates the window and outputs none or several elements.
	 *
	 * @param key The key for which this window is evaluated.
	 * @param context The context in which the window is being evaluated.
	 * @param elements The elements in the window being evaluated.
	 * @param out A collector for emitting elements.
	 *
	 * @throws Exception The function may throw exceptions to fail the program and trigger recovery.
	 */
	public abstract void process(KEY key, Context context, Iterable<IN> elements, Collector<OUT> out) throws Exception;

	/**
	 * Deletes any state in the {@code Context} when the Window expires
	 * (the watermark passes its {@code maxTimestamp} + {@code allowedLateness}).
	 *
	 * @param context The context to which the window is being evaluated
	 * @throws Exception The function may throw exceptions to fail the program and trigger recovery.
	 */
	public void clear(Context context) throws Exception {}

	/**
	 * The context holding window metadata.
	 */
	public abstract class Context implements java.io.Serializable {
		/**
		 * Returns the window that is being evaluated.
		 */
		public abstract W window();

		/** Returns the current processing time. */
		public abstract long currentProcessingTime();

		/** Returns the current event-time watermark. */
		public abstract long currentWatermark();

		/**
		 * State accessor for per-key and per-window state.
		 *
		 * <p><b>NOTE:</b>If you use per-window state you have to ensure that you clean it up
		 * by implementing {@link ProcessWindowFunction#clear(Context)}.
		 */
		public abstract KeyedStateStore windowState();

		/**
		 * State accessor for per-key global state.
		 */
		public abstract KeyedStateStore globalState();

		/**
		 * Emits a record to the side output identified by the {@link OutputTag}.
		 *
		 * @param outputTag the {@code OutputTag} that identifies the side output to emit to.
		 * @param value The record to emit.
		 */
		public abstract <X> void output(OutputTag<X> outputTag, X value);
	}
}
