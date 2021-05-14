

package org.apache.flink.api.java.aggregation;

import org.apache.flink.annotation.Internal;
import org.apache.flink.types.CopyableValue;
import org.apache.flink.types.ResettableValue;

/**
 * Implementation of {@link AggregationFunction} for max operation.
 */
@Internal
public abstract class MaxAggregationFunction<T extends Comparable<T>> extends AggregationFunction<T> {

	@Override
	public String toString() {
		return "MAX";
	}

	// --------------------------------------------------------------------------------------------

	private static final class ImmutableMaxAgg<U extends Comparable<U>> extends MaxAggregationFunction<U> {

		private U value;

		@Override
		public void initializeAggregate() {
			value = null;
		}

		@Override
		public void aggregate(U val) {
			if (value != null) {
				int cmp = value.compareTo(val);
				value = (cmp > 0) ? value : val;
			} else {
				value = val;
			}
		}

		@Override
		public U getAggregate() {
			return value;
		}
	}

	// --------------------------------------------------------------------------------------------

	private static final class MutableMaxAgg<U extends Comparable<U> & ResettableValue<U> & CopyableValue<U>> extends MaxAggregationFunction<U> {
		private static final long serialVersionUID = 1L;

		private U value;

		@Override
		public void initializeAggregate() {
			value = null;
		}

		@Override
		public void aggregate(U val) {
			if (value != null) {
				int cmp = value.compareTo(val);
				if (cmp < 0) {
					value.setValue(val);
				}
			} else {
				value = val.copy();
			}
		}

		@Override
		public U getAggregate() {
			return value;
		}
	}

	// --------------------------------------------------------------------------------------------

	/**
	 * Factory for {@link MaxAggregationFunction}.
	 */
	public static final class MaxAggregationFunctionFactory implements AggregationFunctionFactory {
		private static final long serialVersionUID = 1L;

		@SuppressWarnings({ "unchecked", "rawtypes" })
		@Override
		public <T> AggregationFunction<T> createAggregationFunction(Class<T> type) {
			if (Comparable.class.isAssignableFrom(type)) {
				if (ResettableValue.class.isAssignableFrom(type) && CopyableValue.class.isAssignableFrom(type)) {
					return (AggregationFunction<T>) new MutableMaxAgg();
				} else {
					return (AggregationFunction<T>) new ImmutableMaxAgg();
				}
			} else {
				throw new UnsupportedAggregationTypeException("The type " + type.getName() +
					" is not supported for maximum aggregation. " +
					"Maximum aggregatable types must implement the Comparable interface.");
			}
		}
	}
}
