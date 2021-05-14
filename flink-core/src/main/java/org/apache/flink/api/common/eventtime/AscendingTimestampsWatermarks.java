package org.apache.flink.api.common.eventtime;

import org.apache.flink.annotation.Public;

import java.time.Duration;

/**
 * A watermark generator that assumes monotonically ascending timestamps within the
 * stream split and periodically generates watermarks based on that assumption.
 *
 * <p>The current watermark is always one after the latest (highest) timestamp,
 * because we assume that more records with the same timestamp may still follow.
 *
 * <p>The watermarks are generated periodically and tightly follow the latest
 * timestamp in the data. The delay introduced by this strategy is mainly the periodic
 * interval in which the watermarks are generated, which can be configured via
 * {@link org.apache.flink.api.common.ExecutionConfig#setAutoWatermarkInterval(long)}.
 */
@Public
public class AscendingTimestampsWatermarks<T> extends BoundedOutOfOrdernessWatermarks<T> {

	/**
	 * Creates a new watermark generator with for ascending timestamps.
	 */
	public AscendingTimestampsWatermarks() {
		super(Duration.ofMillis(0));
	}
}
