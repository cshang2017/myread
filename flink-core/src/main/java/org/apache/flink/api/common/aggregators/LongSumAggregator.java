package org.apache.flink.api.common.aggregators;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.types.LongValue;

/**
 * An {@link Aggregator} that sums up long values.
 */
@SuppressWarnings("serial")
@PublicEvolving
public class LongSumAggregator implements Aggregator<LongValue> {

	private long sum;	// the sum
	
	@Override
	public LongValue getAggregate() {
		return new LongValue(sum);
	}

	@Override
	public void aggregate(LongValue element) {
		sum += element.getValue();
	}
	
	/**
	 * Adds the given value to the current aggregate.
	 * 
	 * @param value The value to add to the aggregate.
	 */
	public void aggregate(long value) {
		sum += value;
	}

	@Override
	public void reset() {
		sum = 0;
	}
}
