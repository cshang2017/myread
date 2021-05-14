package org.apache.flink.api.common.operators;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.aggregators.AggregatorRegistry;

@Internal
public interface IterationOperator {
	
	AggregatorRegistry getAggregators();
}
