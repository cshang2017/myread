

package org.apache.flink.api.java.aggregation;

import org.apache.flink.annotation.Internal;
import org.apache.flink.types.CopyableValue;
import org.apache.flink.types.ResettableValue;

/**
 * Implementations of {@link AggregationFunction} for min operation.
 * @param <T> aggregating type
 */
@Internal
public abstract class MinAggregationFunction<T extends Comparable<T>> extends AggregationFunction<T> {

	@Override
	public String toString() {
		return "MIN";
	}

	// --------------------------------------------------------------------------------------------

	private static final class ImmutableMinAgg<U extends Comparable<U>> extends MinAggregationFunction<U> {

		private U value;

		@Override
		public void initializeAggregate() {
			value = null;
		}

		@Override
		public void aggregate(U val) {
			if (value != null) {
				int cmp = value.compareTo(val);
				value = (cmp < 0) ? value : val;
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

	private static final class MutableMinAgg<U extends Comparable<U> & ResettableValue<U> & CopyableValue<U>> extends MinAggregationFunction<U> {

		private U value;

		@Override
		public void initializeAggregate() {
			value = null;
		}

		@Override
		public void aggregate(U val) {
			if (value != null) {
				int cmp = value.compareTo(val);
				if (cmp > 0) {
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


	/**
	 * Factory for {@link MinAggregationFunction}.
	 */
	public static final class MinAggregationFunctionFactory implements AggregationFunctionFactory {
		private static final long serialVersionUID = 1L;

		@SuppressWarnings({ "unchecked", "rawtypes" })
		@Override
		public <T> AggregationFunction<T> createAggregationFunction(Class<T> type) {
			if (Comparable.class.isAssignableFrom(type)) {
				if (ResettableValue.class.isAssignableFrom(type) && CopyableValue.class.isAssignableFrom(type)) {
					return (AggregationFunction<T>) new MutableMinAgg();
				} else {
					return (AggregationFunction<T>) new ImmutableMinAgg();
				}
			} 
		}
	}
}
