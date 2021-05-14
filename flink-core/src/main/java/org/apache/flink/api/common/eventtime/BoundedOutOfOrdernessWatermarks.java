

package org.apache.flink.api.common.eventtime;

import org.apache.flink.annotation.Public;

import java.time.Duration;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A WatermarkGenerator for situations where records are out of order, but you can place an upper
 * bound on how far the events are out of order. An out-of-order bound B means that once an
 * event with timestamp T was encountered, no events older than {@code T - B} will follow any more.
 *
 * <p>The watermarks are generated periodically. The delay introduced by this watermark strategy
 * is the periodic interval length, plus the out-of-orderness bound.
 */
@Public
public class BoundedOutOfOrdernessWatermarks<T> implements WatermarkGenerator<T> {

	/** The maximum timestamp encountered so far. */
	private long maxTimestamp;

	/** The maximum out-of-orderness that this watermark generator assumes. */
	private final long outOfOrdernessMillis;

	/**
	 * Creates a new watermark generator with the given out-of-orderness bound.
	 *
	 * @param maxOutOfOrderness The bound for the out-of-orderness of the event timestamps.
	 */
	public BoundedOutOfOrdernessWatermarks(Duration maxOutOfOrderness) {
		checkNotNull(maxOutOfOrderness, "maxOutOfOrderness");
		checkArgument(!maxOutOfOrderness.isNegative(), "maxOutOfOrderness cannot be negative");

		this.outOfOrdernessMillis = maxOutOfOrderness.toMillis();

		// start so that our lowest watermark would be Long.MIN_VALUE.
		this.maxTimestamp = Long.MIN_VALUE + outOfOrdernessMillis + 1;
	}

	// ------------------------------------------------------------------------

	@Override
	public void onEvent(T event, long eventTimestamp, WatermarkOutput output) {
		maxTimestamp = Math.max(maxTimestamp, eventTimestamp);
	}

	@Override
	public void onPeriodicEmit(WatermarkOutput output) {
		output.emitWatermark(new Watermark(maxTimestamp - outOfOrdernessMillis - 1));
	}
}
