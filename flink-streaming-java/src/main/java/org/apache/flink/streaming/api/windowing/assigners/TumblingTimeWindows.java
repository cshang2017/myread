package org.apache.flink.streaming.api.windowing.assigners;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * A {@link WindowAssigner} that windows elements into windows based on the timestamp of the
 * elements. Windows cannot overlap.
 *
 * @deprecated Please use {@link TumblingEventTimeWindows}.
 */
@PublicEvolving
@Deprecated
public class TumblingTimeWindows extends TumblingEventTimeWindows {

	private TumblingTimeWindows(long size) {
		super(size, 0);
	}

	/**
	 * Creates a new {@code TumblingTimeWindows} {@link WindowAssigner} that assigns
	 * elements to time windows based on the element timestamp.
	 *
	 * @deprecated Please use {@link TumblingEventTimeWindows#of(Time)}.
	 *
	 * @param size The size of the generated windows.
	 * @return The time policy.
	 */
	@Deprecated()
	public static TumblingTimeWindows of(Time size) {
		return new TumblingTimeWindows(size.toMilliseconds());
	}
}
