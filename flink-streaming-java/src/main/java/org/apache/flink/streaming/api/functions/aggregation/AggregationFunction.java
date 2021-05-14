package org.apache.flink.streaming.api.functions.aggregation;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.functions.ReduceFunction;

/**
 * Holder class for aggregation types that can be used on a windowed stream or keyed stream.
 */
@Internal
public abstract class AggregationFunction<T> implements ReduceFunction<T> {
	private static final long serialVersionUID = 1L;

	/**
	 * Aggregation types that can be used on a windowed stream or keyed stream.
	 */
	public enum AggregationType {
		SUM, MIN, MAX, MINBY, MAXBY,
	}
}
