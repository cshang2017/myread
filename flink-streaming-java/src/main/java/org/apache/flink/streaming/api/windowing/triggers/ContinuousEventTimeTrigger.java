

package org.apache.flink.streaming.api.windowing.triggers;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.Window;

/**
 * A {@link Trigger} that continuously fires based on a given time interval. This fires based
 * on {@link org.apache.flink.streaming.api.watermark.Watermark Watermarks}.
 *
 * @see org.apache.flink.streaming.api.watermark.Watermark
 *
 * @param <W> The type of {@link Window Windows} on which this trigger can operate.
 */
@PublicEvolving
public class ContinuousEventTimeTrigger<W extends Window> extends Trigger<Object, W> {

	private final long interval;

	/** When merging we take the lowest of all fire timestamps as the new fire timestamp. */
	private final ReducingStateDescriptor<Long> stateDesc =
			new ReducingStateDescriptor<>("fire-time", new Min(), LongSerializer.INSTANCE);

	private ContinuousEventTimeTrigger(long interval) {
		this.interval = interval;
	}

	@Override
	public TriggerResult onElement(Object element, long timestamp, W window, TriggerContext ctx) throws Exception {

		if (window.maxTimestamp() <= ctx.getCurrentWatermark()) {
			// if the watermark is already past the window fire immediately
			return TriggerResult.FIRE;
		} else {
			ctx.registerEventTimeTimer(window.maxTimestamp());
		}

		ReducingState<Long> fireTimestamp = ctx.getPartitionedState(stateDesc);
		if (fireTimestamp.get() == null) {
			long start = timestamp - (timestamp % interval);
			long nextFireTimestamp = start + interval;
			ctx.registerEventTimeTimer(nextFireTimestamp);
			fireTimestamp.add(nextFireTimestamp);
		}

		return TriggerResult.CONTINUE;
	}

	@Override
	public TriggerResult onEventTime(long time, W window, TriggerContext ctx) throws Exception {

		if (time == window.maxTimestamp()){
			return TriggerResult.FIRE;
		}

		ReducingState<Long> fireTimestampState = ctx.getPartitionedState(stateDesc);

		Long fireTimestamp = fireTimestampState.get();

		if (fireTimestamp != null && fireTimestamp == time) {
			fireTimestampState.clear();
			fireTimestampState.add(time + interval);
			ctx.registerEventTimeTimer(time + interval);
			return TriggerResult.FIRE;
		}

		return TriggerResult.CONTINUE;
	}

	@Override
	public TriggerResult onProcessingTime(long time, W window, TriggerContext ctx) throws Exception {
		return TriggerResult.CONTINUE;
	}

	@Override
	public void clear(W window, TriggerContext ctx) throws Exception {
		ReducingState<Long> fireTimestamp = ctx.getPartitionedState(stateDesc);
		Long timestamp = fireTimestamp.get();
		if (timestamp != null) {
			ctx.deleteEventTimeTimer(timestamp);
			fireTimestamp.clear();
		}
	}

	@Override
	public boolean canMerge() {
		return true;
	}

	@Override
	public void onMerge(W window, OnMergeContext ctx) throws Exception {
		ctx.mergePartitionedState(stateDesc);
		Long nextFireTimestamp = ctx.getPartitionedState(stateDesc).get();
		if (nextFireTimestamp != null) {
			ctx.registerEventTimeTimer(nextFireTimestamp);
		}
	}

	@Override
	public String toString() {
		return "ContinuousEventTimeTrigger(" + interval + ")";
	}

	@VisibleForTesting
	public long getInterval() {
		return interval;
	}

	/**
	 * Creates a trigger that continuously fires based on the given interval.
	 *
	 * @param interval The time interval at which to fire.
	 * @param <W> The type of {@link Window Windows} on which this trigger can operate.
	 */
	public static <W extends Window> ContinuousEventTimeTrigger<W> of(Time interval) {
		return new ContinuousEventTimeTrigger<>(interval.toMilliseconds());
	}

	private static class Min implements ReduceFunction<Long> {
		private static final long serialVersionUID = 1L;

		@Override
		public Long reduce(Long value1, Long value2) throws Exception {
			return Math.min(value1, value2);
		}
	}
}
