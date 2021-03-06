
package org.apache.flink.table.types;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.lang.reflect.Array;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * A data type that contains an element type (e.g. {@code ARRAY} or {@code MULTISET}).
 *
 * @see DataTypes for a list of supported data types
 */
@PublicEvolving
public final class CollectionDataType extends DataType {

	private final DataType elementDataType;

	public CollectionDataType(
			LogicalType logicalType,
			@Nullable Class<?> conversionClass,
			DataType elementDataType) {
		super(logicalType, ensureArrayConversionClass(logicalType, elementDataType, conversionClass));
		this.elementDataType = Preconditions.checkNotNull(elementDataType, "Element data type must not be null.");
	}

	public CollectionDataType(
			LogicalType logicalType,
			DataType elementDataType) {
		this(logicalType, null, elementDataType);
	}

	public DataType getElementDataType() {
		return elementDataType;
	}

	@Override
	public DataType notNull() {
		return new CollectionDataType(
			logicalType.copy(false),
			conversionClass,
			elementDataType);
	}

	@Override
	public DataType nullable() {
		return new CollectionDataType(
			logicalType.copy(true),
			conversionClass,
			elementDataType);
	}

	@Override
	public DataType bridgedTo(Class<?> newConversionClass) {
		return new CollectionDataType(
			logicalType,
			Preconditions.checkNotNull(newConversionClass, "New conversion class must not be null."),
			ensureElementConversionClass(elementDataType, newConversionClass));
	}

	@Override
	public List<DataType> getChildren() {
		return Collections.singletonList(elementDataType);
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
		CollectionDataType that = (CollectionDataType) o;
		return elementDataType.equals(that.elementDataType);
	}

	@Override
	public int hashCode() {
		return Objects.hash(super.hashCode(), elementDataType);
	}

	// --------------------------------------------------------------------------------------------

	private static Class<?> ensureArrayConversionClass(
			LogicalType logicalType,
			DataType elementDataType,
			@Nullable Class<?> clazz) {
		// arrays are a special case because their default conversion class depends on the
		// conversion class of the element type
		if (logicalType.getTypeRoot() == LogicalTypeRoot.ARRAY && clazz == null) {
			return Array.newInstance(elementDataType.getConversionClass(), 0).getClass();
		}
		return clazz;
	}

	private DataType ensureElementConversionClass(
			DataType elementDataType,
			Class<?> clazz) {
		// arrays are a special case because their element conversion class depends on the
		// outer conversion class
		if (logicalType.getTypeRoot() == LogicalTypeRoot.ARRAY && clazz.isArray()) {
			return elementDataType.bridgedTo(clazz.getComponentType());
		}
		return elementDataType;
	}
}
