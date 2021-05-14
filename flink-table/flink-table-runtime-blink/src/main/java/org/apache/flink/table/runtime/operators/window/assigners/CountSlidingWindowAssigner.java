package org.apache.flink.table.runtime.operators.window.assigners;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.operators.window.CountWindow;
import org.apache.flink.table.runtime.operators.window.internal.InternalWindowProcessFunction;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * A {@link WindowAssigner} that windows elements into sliding windows based on the count number
 * of the elements. Windows can possibly overlap.
 */
public class CountSlidingWindowAssigner extends WindowAssigner<CountWindow> {

	private final long windowSize;
	private final long windowSlide;

	private transient ValueState<Long> count;

	private CountSlidingWindowAssigner(long windowSize, long windowSlide) {
		if (windowSize <= 0 || windowSlide <= 0) {
			throw new IllegalArgumentException(
				"SlidingCountWindowAssigner parameters must satisfy slide > 0 and size > 0");
		}
		this.windowSize = windowSize;
		this.windowSlide = windowSlide;
	}

	@Override
	public void open(InternalWindowProcessFunction.Context<?, CountWindow> ctx) throws Exception {
		String descriptorName = "slide-count-assigner";
		ValueStateDescriptor<Long> countDescriptor = new ValueStateDescriptor<>(
			descriptorName,
			Types.LONG);
		this.count = ctx.getPartitionedState(countDescriptor);
	}

	@Override
	public Collection<CountWindow> assignWindows(RowData element, long timestamp) throws IOException {
		Long countValue = count.value();
		long currentCount = countValue == null ? 0L : countValue;
		count.update(currentCount + 1);
		long lastId = currentCount / windowSlide;
		long lastStart = lastId * windowSlide;
		long lastEnd = lastStart + windowSize - 1;
		List<CountWindow> windows = new ArrayList<>();
		while (lastId >= 0 && lastStart <= currentCount && currentCount <= lastEnd) {
			if (lastStart <= currentCount && currentCount <= lastEnd) {
				windows.add(new CountWindow(lastId));
			}
			lastId--;
			lastStart -= windowSlide;
			lastEnd -= windowSlide;
		}
		return windows;
	}

	@Override
	public TypeSerializer<CountWindow> getWindowSerializer(ExecutionConfig executionConfig) {
		return new CountWindow.Serializer();
	}

	@Override
	public boolean isEventTime() {
		return false;
	}

	@Override
	public String toString() {
		return "CountSlidingWindow(" + windowSize + ", " + windowSlide + ")";
	}

	public static CountSlidingWindowAssigner of(long windowSize, long windowSlide) {
		return new CountSlidingWindowAssigner(windowSize, windowSlide);
	}

}
