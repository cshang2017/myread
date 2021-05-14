ackage org.apache.flink.cep.functions;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.List;
import java.util.Map;

/**
 * Enables handling timed out partial matches. It shall be used in a mixin style. If you need your
 * {@link PatternProcessFunction} to be able to handle timed out partial matches implement this interface as well.
 * Example:
 *
 * <pre>
 * {@code
 * private class MyFunction extends PatternProcessFunction<IN, OUT> implements TimedOutPartialMatchHandler<IN> {
 *
 * }
 * }
 * </pre>
 *
 * @param <IN> type of input elements
 */
@PublicEvolving
public interface TimedOutPartialMatchHandler<IN> {

	/**
	 * Called for every timed out partial match (due to {@link org.apache.flink.cep.pattern.Pattern#within(Time)}).
	 * It enables custom handling, e.g. one can emit the timed out results through a side output:
	 *
	 * <pre>
	 * {@code
	 *
	 * private final OutputTag<T> timedOutPartialMatchesTag = ...
	 *
	 * private class MyFunction extends PatternProcessFunction<IN, OUT> implements TimedOutPartialMatchHandler<IN> {
	 *
	 *     @Override
	 *     public void processMatch(Map<String, List<IN>> match, Context ctx, Collector<OUT> out) throws Exception {
	 *          ...
	 *     }
	 *
	 *     @Override
	 *     void processTimedOutMatch(Map<String, List<IN>> match, PatternProcessFunction.Context ctx) throws Exception {
	 *          ctx.output(timedOutPartialMatchesTag, match);
	 *     }
	 * }
	 * }
	 * </pre>
	 *
	 * <p>{@link PatternProcessFunction.Context#timestamp()} in this case returns the minimal time in which we can
	 * say that the partial match will not become a match, which is effectively the timestamp of the first element
	 * assigned to the partial match plus the value of within.
	 *
	 * @param match map containing the timed out partial match. Events are identified by their names.
	 * @param ctx enables access to time features and emitting results through side outputs
	 * @throws Exception This method may throw exceptions. Throwing an exception will cause the
	 *                   operation to fail and may trigger recovery.
	 */
	void processTimedOutMatch(
		final Map<String, List<IN>> match,
		final PatternProcessFunction.Context ctx) throws Exception;
}
