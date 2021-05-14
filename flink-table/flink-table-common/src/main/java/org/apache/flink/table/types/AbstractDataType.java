

package org.apache.flink.table.types;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.logical.LogicalType;

/**
 * Highest abstraction that describes the data type of a value in the table ecosystem. This class
 * describes two kinds of data types:
 *
 * <p>Fully resolved data types that can be used directly to declare input and/or output types of
 * operations. This kind is represented in subclasses of {@link DataType}.
 *
 * <p>Partially resolved data types that can be resolved to {@link DataType} but require a lookup in
 * a catalog or configuration first. This kind is represented in subclasses of {@link UnresolvedDataType}.
 *
 * <p>Note: Use {@link DataTypes} for producing instances of this class.
 *
 * @param <T> kind of data type returned after mutation
 */
@PublicEvolving
public interface AbstractDataType<T extends AbstractDataType<T>> {

	/**
	 * Adds a hint that null values are not expected in the data for this type.
	 *
	 * @return a new, reconfigured data type instance
	 */
	T notNull();

	/**
	 * Adds a hint that null values are expected in the data for this type (default behavior).
	 *
	 * <p>This method exists for explicit declaration of the default behavior or for invalidation of
	 * a previous call to {@link #notNull()}.
	 *
	 * @return a new, reconfigured data type instance
	 */
	T nullable();

	/**
	 * Adds a hint that data should be represented using the given class when entering or leaving
	 * the table ecosystem.
	 *
	 * <p>A supported conversion class depends on the logical type and its nullability property.
	 *
	 * <p>Please see the implementation of {@link LogicalType#supportsInputConversion(Class)},
	 * {@link LogicalType#supportsOutputConversion(Class)}, or the documentation for more information
	 * about supported conversions.
	 *
	 * @return a new, reconfigured data type instance
	 */
	T bridgedTo(Class<?> newConversionClass);
}
