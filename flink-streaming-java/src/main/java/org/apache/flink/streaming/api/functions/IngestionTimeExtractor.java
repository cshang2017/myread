

package org.apache.flink.streaming.api.functions;

import org.apache.flink.streaming.api.watermark.Watermark;

/**
 * A timestamp assigner that assigns timestamps based on the machine's wall clock.
 *
 * <p>If this assigner is used after a stream source, it realizes "ingestion time" semantics.
 *
 * @param <T> The elements that get timestamps assigned.
 */
@Deprecated
public class IngestionTimeExtractor<T> implements AssignerWithPeriodicWatermarks<T> {

	private long maxTimestamp;

	@Override
	public long extractTimestamp(T element, long previousElementTimestamp) {
		// make sure timestamps are monotonously increasing, even when the system clock re-syncs
		final long now = Math.max(System.currentTimeMillis(), maxTimestamp);
		maxTimestamp = now;
		return now;
	}

	@Override
	public Watermark getCurrentWatermark() {
		// make sure timestamps are monotonously increasing, even when the system clock re-syncs
		final long now = Math.max(System.currentTimeMillis(), maxTimestamp);
		maxTimestamp = now;
		return new Watermark(now - 1);
	}
}
