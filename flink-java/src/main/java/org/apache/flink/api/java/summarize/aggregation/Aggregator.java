

package org.apache.flink.api.java.summarize.aggregation;

import org.apache.flink.annotation.Internal;

/**
 * Generic interface for aggregation.
 *
 * @param <T> the type to be aggregated
 * @param <R> the result type of the aggregation
 */
@Internal
public interface Aggregator<T, R> extends java.io.Serializable {

	/** Add a value to the current aggregation. */
	void aggregate(T value);

	/**
	 * Combine two aggregations of the same type.
	 *
	 * <p>(Implementations will need to do an unchecked cast).
	 */
	void combine(Aggregator<T, R> otherSameType);

	/** Provide the final result of the aggregation. */
	R result();
}
