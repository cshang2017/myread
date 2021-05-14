

package org.apache.flink.api.java.aggregation;

import org.apache.flink.annotation.Internal;

/**
 * @param <T> The type to be aggregated.
 */
@Internal
public abstract class AggregationFunction<T> implements java.io.Serializable {

	public abstract void initializeAggregate();

	public abstract void aggregate(T value);

	public abstract T getAggregate();
}
