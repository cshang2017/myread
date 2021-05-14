package org.apache.flink.api.common.eventtime;

import org.apache.flink.annotation.Public;

/**
 * A {@link TimestampAssigner} that forwards the already-assigned timestamp. This is for use when
 * records come out of a source with valid timestamps, for example from the Kafka Metadata.
 */
@Public
public final class RecordTimestampAssigner<E> implements TimestampAssigner<E> {

	@Override
	public long extractTimestamp(E element, long recordTimestamp) {
		return recordTimestamp;
	}
}
