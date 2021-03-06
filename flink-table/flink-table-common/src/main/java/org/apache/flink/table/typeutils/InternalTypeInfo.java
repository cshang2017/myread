package org.apache.flink.table.typeutils;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.AtomicType;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;

import java.util.Objects;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Type information for internal types of the Table API that are for translation purposes only
 * and should not be contained in final plan.
 */
@Internal
public abstract class InternalTypeInfo<T> extends TypeInformation<T> implements AtomicType<T> {


	public final Class<T> clazz;

	public InternalTypeInfo(Class<T> clazz) {
		this.clazz = checkNotNull(clazz);
	}

	@Override
	public boolean isBasicType() {
		throw new UnsupportedOperationException("This type is for internal use only.");
	}

	@Override
	public boolean isTupleType() {
		throw new UnsupportedOperationException("This type is for internal use only.");
	}

	@Override
	public int getArity() {
		throw new UnsupportedOperationException("This type is for internal use only.");
	}

	@Override
	public int getTotalFields() {
		throw new UnsupportedOperationException("This type is for internal use only.");
	}

	@Override
	public Class<T> getTypeClass() {
		return clazz;
	}

	@Override
	public boolean isKeyType() {
		throw new UnsupportedOperationException("This type is for internal use only.");
	}

	@Override
	public TypeSerializer<T> createSerializer(ExecutionConfig config) {
		throw new UnsupportedOperationException("This type is for internal use only.");
	}

	@Override
	public TypeComparator<T> createComparator(
			boolean sortOrderAscending,
			ExecutionConfig executionConfig) {
		throw new UnsupportedOperationException("This type is for internal use only.");
	}

	// ----------------------------------------------------------------------------------------------

	@Override
	public int hashCode() {
		return Objects.hash(clazz);
	}

	public abstract boolean canEqual(Object obj);

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof InternalTypeInfo) {
			InternalTypeInfo other = (InternalTypeInfo) obj;
			return other.canEqual(this) && this.clazz.equals(other.clazz);
		} else {
			return false;
		}
	}

	@Override
	public String toString() {
		return getClass().getSimpleName();
	}
}
