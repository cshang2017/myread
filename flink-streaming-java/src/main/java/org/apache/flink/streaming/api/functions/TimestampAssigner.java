

package org.apache.flink.streaming.api.functions;

import org.apache.flink.api.common.functions.Function;

/**
 * A {@code TimestampAssigner} assigns event time timestamps to elements.
 * These timestamps are used by all functions that operate on event time,
 * for example event time windows.
 *
 * <p>Timestamps are represented in milliseconds since the Epoch
 * (midnight, January 1, 1970 UTC).
 *
 * @param <T> The type of the elements to which this assigner assigns timestamps.
 *
 * @deprecated use {@link org.apache.flink.api.common.eventtime.TimestampAssigner}
 */
@Deprecated
public interface TimestampAssigner<T> extends org.apache.flink.api.common.eventtime.TimestampAssigner<T>, Function {

	/**
	 * Assigns a timestamp to an element, in milliseconds since the Epoch.
	 *
	 * <p>The method is passed the previously assigned timestamp of the element.
	 * That previous timestamp may have been assigned from a previous assigner,
	 * by ingestion time. If the element did not carry a timestamp before, this value is
	 * {@code Long.MIN_VALUE}.
	 *
	 * @param element The element that the timestamp will be assigned to.
	 * @param recordTimestamp The previous internal timestamp of the element,
	 *                                 or a negative value, if no timestamp has been assigned yet.
	 * @return The new timestamp.
	 */
	@Override
	long extractTimestamp(T element, long recordTimestamp);
}
