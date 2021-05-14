package org.apache.flink.table.types.inference;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.types.DataType;

import java.util.Optional;

/**
 * Strategy for inferring the data type of a function call. The inferred type might describe the
 * final result or an intermediate result (accumulation type) of a function.
 *
 * <p>Note: Implementations should implement {@link Object#hashCode()} and {@link Object#equals(Object)}.
 *
 * @see TypeStrategies
 */
@PublicEvolving
public interface TypeStrategy {

	/**
	 * Infers a type from the given function call.
	 */
	Optional<DataType> inferType(CallContext callContext);
}
