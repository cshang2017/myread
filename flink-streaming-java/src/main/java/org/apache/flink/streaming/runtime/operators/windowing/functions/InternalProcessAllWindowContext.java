

package org.apache.flink.streaming.runtime.operators.windowing.functions;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.state.KeyedStateStore;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.OutputTag;

/**
 * Internal reusable context wrapper.
 *
 * @param <IN> The type of the input value.
 * @param <OUT> The type of the output value.
 * @param <W> The type of the window.
 */
@Internal
public class InternalProcessAllWindowContext<IN, OUT, W extends Window>
	extends ProcessAllWindowFunction<IN, OUT, W>.Context {

	W window;
	InternalWindowFunction.InternalWindowContext internalContext;

	InternalProcessAllWindowContext(ProcessAllWindowFunction<IN, OUT, W> function) {
		function.super();
	}

	@Override
	public W window() {
		return window;
	}

	@Override
	public KeyedStateStore windowState() {
		return internalContext.windowState();
	}

	@Override
	public KeyedStateStore globalState() {
		return internalContext.globalState();
	}

	@Override
	public <X> void output(OutputTag<X> outputTag, X value) {
		internalContext.output(outputTag, value);
	}
}
