

package org.apache.flink.api.java.aggregation;

import org.apache.flink.annotation.Public;

/**
 * Shortcuts for Aggregation factories.
 */
@Public
public enum Aggregations {

	SUM (new SumAggregationFunction.SumAggregationFunctionFactory()),
	MIN (new MinAggregationFunction.MinAggregationFunctionFactory()),
	MAX (new MaxAggregationFunction.MaxAggregationFunctionFactory());

	// --------------------------------------------------------------------------------------------

	private final AggregationFunctionFactory factory;

	private Aggregations(AggregationFunctionFactory factory) {
		this.factory = factory;
	}

	public AggregationFunctionFactory getFactory() {
		return this.factory;
	}

}
