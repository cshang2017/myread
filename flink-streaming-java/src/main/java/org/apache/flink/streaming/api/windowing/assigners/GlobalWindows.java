

package org.apache.flink.streaming.api.windowing.assigners;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;

import java.util.Collection;
import java.util.Collections;

/**
 * A {@link WindowAssigner} that assigns all elements to the same {@link GlobalWindow}.
 *
 * <p>Use this if you want to use a {@link Trigger} and
 * {@link org.apache.flink.streaming.api.windowing.evictors.Evictor} to do flexible, policy based
 * windows.
 */
@PublicEvolving
public class GlobalWindows extends WindowAssigner<Object, GlobalWindow> {

	private GlobalWindows() {}

	@Override
	public Collection<GlobalWindow> assignWindows(Object element, long timestamp, WindowAssignerContext context) {
		return Collections.singletonList(GlobalWindow.get());
	}

	@Override
	public Trigger<Object, GlobalWindow> getDefaultTrigger(StreamExecutionEnvironment env) {
		return new NeverTrigger();
	}

	@Override
	public String toString() {
		return "GlobalWindows()";
	}

	/**
	 * Creates a new {@code GlobalWindows} {@link WindowAssigner} that assigns
	 * all elements to the same {@link GlobalWindow}.
	 *
	 * @return The global window policy.
	 */
	public static GlobalWindows create() {
		return new GlobalWindows();
	}

	/**
	 * A trigger that never fires, as default Trigger for GlobalWindows.
	 */
	@Internal
	public static class NeverTrigger extends Trigger<Object, GlobalWindow> {
		private static final long serialVersionUID = 1L;

		@Override
		public TriggerResult onElement(Object element, long timestamp, GlobalWindow window, TriggerContext ctx) {
			return TriggerResult.CONTINUE;
		}

		@Override
		public TriggerResult onEventTime(long time, GlobalWindow window, TriggerContext ctx) {
			return TriggerResult.CONTINUE;
		}

		@Override
		public TriggerResult onProcessingTime(long time, GlobalWindow window, TriggerContext ctx) {
			return TriggerResult.CONTINUE;
		}

		@Override
		public void clear(GlobalWindow window, TriggerContext ctx) throws Exception {}

		@Override
		public void onMerge(GlobalWindow window, OnMergeContext ctx) {
		}
	}

	@Override
	public TypeSerializer<GlobalWindow> getWindowSerializer(ExecutionConfig executionConfig) {
		return new GlobalWindow.Serializer();
	}

	@Override
	public boolean isEventTime() {
		return false;
	}
}
