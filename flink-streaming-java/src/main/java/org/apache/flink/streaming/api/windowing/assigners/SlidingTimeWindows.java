package org.apache.flink.streaming.api.windowing.assigners;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * A {@link WindowAssigner} that windows elements into sliding windows based on the timestamp of the
 * elements. Windows can possibly overlap.
 *
 * @deprecated Please use {@link SlidingEventTimeWindows}.
 */
@PublicEvolving
@Deprecated
public class SlidingTimeWindows extends SlidingEventTimeWindows {
	private static final long serialVersionUID = 1L;

	private SlidingTimeWindows(long size, long slide) {
		super(size, slide, 0);
	}

	/**
	 * Creates a new {@code SlidingTimeWindows} {@link WindowAssigner} that assigns
	 * elements to sliding time windows based on the element timestamp.
	 *
	 * @deprecated Please use {@link SlidingEventTimeWindows#of(Time, Time)}.
	 *
	 * @param size The size of the generated windows.
	 * @param slide The slide interval of the generated windows.
	 * @return The time policy.
	 */
	@Deprecated()
	public static SlidingTimeWindows of(Time size, Time slide) {
		return new SlidingTimeWindows(size.toMilliseconds(), slide.toMilliseconds());
	}
}
