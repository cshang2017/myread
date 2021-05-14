package org.apache.flink.table.functions;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.typeinfo.TypeInformation;

/**
 * Base class for user-defined aggregates and table aggregates.
 */
@PublicEvolving
public abstract class UserDefinedAggregateFunction<T, ACC> extends UserDefinedFunction {

	/**
	 * Creates and initializes the accumulator for this {@link UserDefinedAggregateFunction}. The
	 * accumulator is used to keep the aggregated values which are needed to compute an aggregation
	 * result.
	 *
	 * @return the accumulator with the initial value
	 */
	public abstract ACC createAccumulator();

	/**
	 * Returns the {@link TypeInformation} of the {@link UserDefinedAggregateFunction}'s result.
	 *
	 * @return The {@link TypeInformation} of the {@link UserDefinedAggregateFunction}'s result or
	 *         <code>null</code> if the result type should be automatically inferred.
	 */
	public TypeInformation<T> getResultType() {
		return null;
	}

	/**
	 * Returns the {@link TypeInformation} of the {@link UserDefinedAggregateFunction}'s accumulator.
	 *
	 * @return The {@link TypeInformation} of the {@link UserDefinedAggregateFunction}'s accumulator
	 *         or <code>null</code> if the accumulator type should be automatically inferred.
	 */
	public TypeInformation<ACC> getAccumulatorType() {
		return null;
	}
}
