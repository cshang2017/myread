package org.apache.flink.table.runtime.typeutils;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Preconditions;

import java.util.Map;

/**
 * Base type information for maps.
 *
 * @param <K> The type of the keys in the map.
 * @param <V> The type of the values in the map.
 */
abstract class AbstractMapTypeInfo<K, V, M extends Map<K, V>> extends TypeInformation<M> {

	/* The type information for the keys in the map*/
	final TypeInformation<K> keyTypeInfo;

	/* The type information for the values in the map */
	final TypeInformation<V> valueTypeInfo;

	/**
	 * Constructor with given type information for the keys and the values in
	 * the map.
	 *
	 * @param keyTypeInfo The type information for the keys in the map.
	 * @param valueTypeInfo The type information for the values in th map.
	 */
	AbstractMapTypeInfo(TypeInformation<K> keyTypeInfo, TypeInformation<V> valueTypeInfo) {
		Preconditions.checkNotNull(keyTypeInfo, "The type information for the keys cannot be null.");
		Preconditions.checkNotNull(valueTypeInfo, "The type information for the values cannot be null.");
		this.keyTypeInfo = keyTypeInfo;
		this.valueTypeInfo = valueTypeInfo;
	}

	/**
	 * Constructor with the classes of the keys and the values in the map.
	 *
	 * @param keyClass The class of the keys in the map.
	 * @param valueClass The class of the values in the map.
	 */
	AbstractMapTypeInfo(Class<K> keyClass, Class<V> valueClass) {
		Preconditions.checkNotNull(keyClass, "The key class cannot be null.");
		Preconditions.checkNotNull(valueClass, "The value class cannot be null.");

		this.keyTypeInfo = TypeInformation.of(keyClass);
		this.valueTypeInfo = TypeInformation.of(valueClass);
	}

	// ------------------------------------------------------------------------

	/**
	 * Returns the type information for the keys in the map.
	 *
	 * @return The type information for the keys in the map.
	 */
	public TypeInformation<K> getKeyTypeInfo() {
		return keyTypeInfo;
	}

	/**
	 * Returns the type information for the values in the map.
	 *
	 * @return The type information for the values in the map.
	 */
	public TypeInformation<V> getValueTypeInfo() {
		return valueTypeInfo;
	}

	// ------------------------------------------------------------------------

	@Override
	public boolean isBasicType() {
		return false;
	}

	@Override
	public boolean isTupleType() {
		return false;
	}

	@Override
	public int getArity() {
		return 0;
	}

	@Override
	public int getTotalFields() {
		return 2;
	}

	@Override
	public boolean isKeyType() {
		return false;
	}

	// ------------------------------------------------------------------------

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}

		if (o == null || getClass() != o.getClass()) {
			return false;
		}

		AbstractMapTypeInfo<?, ?, ?> that = (AbstractMapTypeInfo<?, ?, ?>) o;

		return keyTypeInfo.equals(that.keyTypeInfo) && valueTypeInfo.equals(that.valueTypeInfo);
	}

	@Override
	public int hashCode() {
		int result = keyTypeInfo.hashCode();
		result = 31 * result + valueTypeInfo.hashCode();
		return result;
	}
}
