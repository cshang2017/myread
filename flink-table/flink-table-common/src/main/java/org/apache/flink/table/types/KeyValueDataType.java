
package org.apache.flink.table.types;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * A data type that contains a key and value data type (e.g. {@code MAP}).
 *
 * @see DataTypes for a list of supported data types
 */
@PublicEvolving
public final class KeyValueDataType extends DataType {

	private final DataType keyDataType;

	private final DataType valueDataType;

	public KeyValueDataType(
			LogicalType logicalType,
			@Nullable Class<?> conversionClass,
			DataType keyDataType,
			DataType valueDataType) {
		super(logicalType, conversionClass);
		this.keyDataType = Preconditions.checkNotNull(keyDataType, "Key data type must not be null.");
		this.valueDataType = Preconditions.checkNotNull(valueDataType, "Value data type must not be null.");
	}

	public KeyValueDataType(
			LogicalType logicalType,
			DataType keyDataType,
			DataType valueDataType) {
		this(logicalType, null, keyDataType, valueDataType);
	}

	public DataType getKeyDataType() {
		return keyDataType;
	}

	public DataType getValueDataType() {
		return valueDataType;
	}

	@Override
	public DataType notNull() {
		return new KeyValueDataType(
			logicalType.copy(false),
			conversionClass,
			keyDataType,
			valueDataType);
	}

	@Override
	public DataType nullable() {
		return new KeyValueDataType(
			logicalType.copy(true),
			conversionClass,
			keyDataType,
			valueDataType);
	}

	@Override
	public DataType bridgedTo(Class<?> newConversionClass) {
		return new KeyValueDataType(
			logicalType,
			Preconditions.checkNotNull(newConversionClass, "New conversion class must not be null."),
			keyDataType,
			valueDataType);
	}

	@Override
	public List<DataType> getChildren() {
		return Arrays.asList(keyDataType, valueDataType);
	}

	@Override
	public <R> R accept(DataTypeVisitor<R> visitor) {
		return visitor.visit(this);
	}
	
}
