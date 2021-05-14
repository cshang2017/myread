

package org.apache.flink.api.java.aggregation;

import org.apache.flink.annotation.Internal;

/**
 * Interface of factory for creating {@link AggregationFunction}.
 */
@Internal
public interface AggregationFunctionFactory extends java.io.Serializable {

	<T> AggregationFunction<T> createAggregationFunction(Class<T> type);

}
