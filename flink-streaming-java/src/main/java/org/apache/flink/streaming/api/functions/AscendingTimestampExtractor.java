

package org.apache.flink.streaming.api.functions;

import org.apache.flink.annotation.PublicEvolving;

/**
 * A timestamp assigner and watermark generator for streams where timestamps are monotonously
 * ascending. In this case, the local watermarks for the streams are easy to generate, because
 * they strictly follow the timestamps.
 *
 * <p><b>Note:</b> This is just a deprecated stub class. The actual code for this has moved to
 * {@link org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor}.
 *
 * @param <T> The type of the elements that this function can extract timestamps from
 *
 * @deprecated Extend {@link org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor} instead.
 */
@PublicEvolving
@Deprecated
public abstract class AscendingTimestampExtractor<T>
	extends org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor<T> {

}
