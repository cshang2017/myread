
package org.apache.flink.table.annotation;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.types.inference.InputTypeStrategies;
import org.apache.flink.table.types.logical.LogicalTypeFamily;

/**
 * A list of commonly used pre-defined groups of similar types for accepting more than just one data
 * type as an input argument in {@link DataTypeHint}s.
 *
 * <p>This list exposes a combination of {@link LogicalTypeFamily} and {@link InputTypeStrategies}
 * via annotations for convenient inline usage.
 */
@PublicEvolving
public enum InputGroup {

	/**
	 * Default if no group is specified.
	 */
	UNKNOWN,

	/**
	 * Enables input wildcards. Any data type can be passed. The behavior is equal to {@link InputTypeStrategies#ANY}.
	 *
	 * <p>Note: The class of the annotated element must be {@link Object} as this is the super class
	 * of all possibly passed data types.
	 */
	ANY
}
