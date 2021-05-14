package org.apache.flink.table.types;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Objects;

/**
 * A data type that contains field data types (i.e. row, structured, and distinct types).
 *
 * @see DataTypes for a list of supported data types
 */
@PublicEvolving
public final class FieldsDataType extends DataType {

	private final List<DataType> fieldDataTypes;

	public FieldsDataType(
			LogicalType logicalType,
			@Nullable Class<?> conversionClass,
			List<DataType> fieldDataTypes) {
		super(logicalType, conversionClass);
		this.fieldDataTypes = Preconditions.checkNotNull(
			fieldDataTypes,
			"Field data types must not be null.");
	}

	public FieldsDataType(
			LogicalType logicalType,
			List<DataType> fieldDataTypes) {
		this(logicalType, null, fieldDataTypes);
	}

	@Override
	public DataType notNull() {
		return new FieldsDataType(
			logicalType.copy(false),
			conversionClass,
			fieldDataTypes);
	}

	@Override
	public DataType nullable() {
		return new FieldsDataType(
			logicalType.copy(true),
			conversionClass,
			fieldDataTypes);
	}

	@Override
	public DataType bridgedTo(Class<?> newConversionClass) {
		return new FieldsDataType(
			logicalType,
			Preconditions.checkNotNull(newConversionClass, "New conversion class must not be null."),
			fieldDataTypes);
	}

	@Override
	public List<DataType> getChildren() {
		return fieldDataTypes;
	}

	@Override
	public <R> R accept(DataTypeVisitor<R> visitor) {
		return visitor.visit(this);
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		if (!super.equals(o)) {
			return false;
		}
		FieldsDataType that = (FieldsDataType) o;
		return fieldDataTypes.equals(that.fieldDataTypes);
	}

	@Override
	public int hashCode() {
		return Objects.hash(super.hashCode(), fieldDataTypes);
	}
}
