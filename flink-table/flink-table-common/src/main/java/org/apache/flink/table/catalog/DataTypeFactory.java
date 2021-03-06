package org.apache.flink.table.catalog;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.types.AbstractDataType;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.UnresolvedDataType;
import org.apache.flink.table.types.logical.DistinctType;
import org.apache.flink.table.types.logical.StructuredType;

/**
 * Factory for creating fully resolved data types that can be used for planning.
 *
 * <p>The factory is useful for types that cannot be created with one of the static methods in
 * {@link DataTypes}) because they require access to configuration or catalog.
 */
@PublicEvolving
public interface DataTypeFactory {

	/**
	 * Creates a type out of an {@link AbstractDataType}.
	 *
	 * <p>If the given type is already a {@link DataType}, the factory will return it unmodified. In
	 * case of {@link UnresolvedDataType}, the factory will resolve it to a {@link DataType}.
	 */
	DataType createDataType(AbstractDataType<?> abstractDataType);

	/**
	 * Creates a type by a fully or partially defined name.
	 *
	 * <p>The factory will parse and resolve the name of a type to a {@link DataType}. This includes
	 * both built-in types as well as user-defined types (see {@link DistinctType} and {@link StructuredType}).
	 */
	DataType createDataType(String name);

	/**
	 * Creates a type by a fully or partially defined identifier.
	 *
	 * <p>The factory will parse and resolve the name of a type to a {@link DataType}. This includes
	 * both built-in types as well as user-defined types (see {@link DistinctType} and {@link StructuredType}).
	 */
	DataType createDataType(UnresolvedIdentifier identifier);

	/**
	 * Creates a type by analyzing the given class.
	 *
	 * <p>It does this by using Java reflection which can be supported by {@link DataTypeHint} annotations
	 * for nested, structured types.
	 *
	 * <p>It will throw an {@link ValidationException} in cases where the reflective extraction needs
	 * more information or simply fails.
	 *
	 * <p>See {@link DataTypes#of(Class)} for further examples.
	 */
	<T> DataType createDataType(Class<T> clazz);

	/**
	 * Creates a RAW type for the given class in cases where no serializer is known and a generic serializer
	 * should be used. The factory will create {@link DataTypes#RAW(Class, TypeSerializer)} with Flink's
	 * default RAW serializer that is automatically configured.
	 *
	 * <p>Note: This type is a black box within the table ecosystem and is only deserialized at the edges
	 * of the API.
	 */
	<T> DataType createRawDataType(Class<T> clazz);
}
