

package org.apache.flink.api.common.eventtime;

import org.apache.flink.annotation.Public;

/**
 * A timestamp assigner that assigns timestamps based on the machine's wall clock.
 * If this assigner is used after a stream source, it realizes "ingestion time" semantics.
 *
 * @param <T> The type of the elements that get timestamps assigned.
 */
@Public
public final class IngestionTimeAssigner<T> implements TimestampAssigner<T> {

	private long maxTimestamp;

	@Override
	public long extractTimestamp(T element, long recordTimestamp) {
		// make sure timestamps are monotonously increasing, even when the system clock re-syncs
		final long now = Math.max(System.currentTimeMillis(), maxTimestamp);
		maxTimestamp = now;
		return now;
	}
}
