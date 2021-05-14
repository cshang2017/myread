package org.apache.flink.table.runtime.operators.window.assigners;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.operators.window.TimeWindow;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.NavigableSet;

/**
 * A {@link WindowAssigner} that windows elements into sessions based on the timestamp.
 * Windows cannot overlap.
 */
public class SessionWindowAssigner extends MergingWindowAssigner<TimeWindow> implements InternalTimeWindowAssigner {

	private final long sessionGap;

	private final boolean isEventTime;

	protected SessionWindowAssigner(long sessionGap, boolean isEventTime) {
		if (sessionGap <= 0) {
			throw new IllegalArgumentException("SessionWindowAssigner parameters must satisfy 0 < size");
		}
		this.sessionGap = sessionGap;
		this.isEventTime = isEventTime;
	}

	@Override
	public Collection<TimeWindow> assignWindows(RowData element, long timestamp) {
		return Collections.singletonList(new TimeWindow(timestamp, timestamp + sessionGap));
	}

	@Override
	public void mergeWindows(TimeWindow newWindow, NavigableSet<TimeWindow> sortedWindows, MergeCallback<TimeWindow> callback) {
		TimeWindow ceiling = sortedWindows.ceiling(newWindow);
		TimeWindow floor = sortedWindows.floor(newWindow);

		Collection<TimeWindow> mergedWindows = new HashSet<>();
		TimeWindow mergeResult = newWindow;
		if (ceiling != null) {
			mergeResult = mergeWindow(mergeResult, ceiling, mergedWindows);
		}
		if (floor != null) {
			mergeResult = mergeWindow(mergeResult, floor, mergedWindows);
		}
		if (!mergedWindows.isEmpty()) {
			// merge happens, add newWindow into the collection as well.
			mergedWindows.add(newWindow);
			callback.merge(mergeResult, mergedWindows);
		}
	}

	/**
	 * Merge curWindow and other, return a new window which covers curWindow and other
	 * if they are overlapped. Otherwise, returns the curWindow itself.  */
	private TimeWindow mergeWindow(TimeWindow curWindow, TimeWindow other, Collection<TimeWindow> mergedWindow) {
		if (curWindow.intersects(other)) {
			mergedWindow.add(other);
			return curWindow.cover(other);
		} else {
			return curWindow;
		}
	}

	@Override
	public TypeSerializer<TimeWindow> getWindowSerializer(ExecutionConfig executionConfig) {
		return new TimeWindow.Serializer();
	}

	@Override
	public boolean isEventTime() {
		return isEventTime;
	}

	@Override
	public String toString() {
		return "SessionWindow(" + sessionGap + ")";
	}

	// ------------------------------------------------------------------------
	//  Utilities
	// ------------------------------------------------------------------------

	/**
	 * Creates a new {@code SessionWindowAssigner} {@link WindowAssigner} that assigns
	 * elements to sessions based on the timestamp.
	 *
	 * @param size The session timeout, i.e. the time gap between sessions
	 * @return The policy.
	 */
	public static SessionWindowAssigner withGap(Duration size) {
		return new SessionWindowAssigner(size.toMillis(), true);
	}

	public SessionWindowAssigner withEventTime() {
		return new SessionWindowAssigner(sessionGap, true);
	}

	public SessionWindowAssigner withProcessingTime() {
		return new SessionWindowAssigner(sessionGap, false);
	}
}
