package org.apache.flink.streaming.api.windowing.assigners;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import java.util.Collection;

/**
 * A base {@link WindowAssigner} used to instantiate one of the deprecated operators.
 *
 * @deprecated will be removed in a future version. please use other {@link WindowAssigner}s listed under
 * {@link org.apache.flink.streaming.api.windowing.assigners}.
 */
@Deprecated
public class BaseAlignedWindowAssigner extends WindowAssigner<Object, TimeWindow> {

	private static final long serialVersionUID = -6214980179706960234L;

	private final long size;

	protected BaseAlignedWindowAssigner(long size) {
		this.size = size;
	}

	public long getSize() {
		return size;
	}

	@Override
	public Collection<TimeWindow> assignWindows(Object element, long timestamp, WindowAssignerContext context) {
		throw new UnsupportedOperationException("This assigner should not be used with the WindowOperator.");
	}

	@Override
	public Trigger<Object, TimeWindow> getDefaultTrigger(StreamExecutionEnvironment env) {
		throw new UnsupportedOperationException("This assigner should not be used with the WindowOperator.");
	}

	@Override
	public TypeSerializer<TimeWindow> getWindowSerializer(ExecutionConfig executionConfig) {
		throw new UnsupportedOperationException("This assigner should not be used with the WindowOperator.");
	}

	@Override
	public boolean isEventTime() {
		throw new UnsupportedOperationException("This assigner should not be used with the WindowOperator.");
	}
}
