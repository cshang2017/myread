package org.apache.flink.runtime.query;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.internal.InternalKvState;
import org.apache.flink.util.Preconditions;

import java.util.Objects;

/**
 * Metadata about a {@link InternalKvState}. This includes the serializers for
 * the key, the namespace, and the values kept in the state.
 *
 * @param <K>	The type of key the state is associated to
 * @param <N>	The type of the namespace the state is associated to
 * @param <V>	The type of values kept internally in state
 */
public class KvStateInfo<K, N, V> {

	private final TypeSerializer<K> keySerializer;
	private final TypeSerializer<N> namespaceSerializer;
	private final TypeSerializer<V> stateValueSerializer;

	public KvStateInfo(
			final TypeSerializer<K> keySerializer,
			final TypeSerializer<N> namespaceSerializer,
			final TypeSerializer<V> stateValueSerializer
	) {
		this.keySerializer = Preconditions.checkNotNull(keySerializer);
		this.namespaceSerializer = Preconditions.checkNotNull(namespaceSerializer);
		this.stateValueSerializer = Preconditions.checkNotNull(stateValueSerializer);
	}

	/**
	 * @return The serializer for the key the state is associated to.
	 */
	public TypeSerializer<K> getKeySerializer() {
		return keySerializer;
	}

	/**
	 * @return The serializer for the namespace the state is associated to.
	 */
	public TypeSerializer<N> getNamespaceSerializer() {
		return namespaceSerializer;
	}

	/**
	 * @return The serializer for the values kept in the state.
	 */
	public TypeSerializer<V> getStateValueSerializer() {
		return stateValueSerializer;
	}

	/**
	 * Creates a deep copy of the current {@link KvStateInfo} by duplicating
	 * all the included serializers.
	 *
	 * <p>This method assumes correct implementation of the {@link TypeSerializer#duplicate()}
	 * method of the included serializers.
	 */
	public KvStateInfo<K, N, V> duplicate() {
		final TypeSerializer<K> dupKeySerializer = keySerializer.duplicate();
		final TypeSerializer<N> dupNamespaceSerializer = namespaceSerializer.duplicate();
		final TypeSerializer<V> dupSVSerializer = stateValueSerializer.duplicate();

		if (
			dupKeySerializer == keySerializer &&
			dupNamespaceSerializer == namespaceSerializer &&
			dupSVSerializer == stateValueSerializer
		) {
			return this;
		}

		return new KvStateInfo<>(dupKeySerializer, dupNamespaceSerializer, dupSVSerializer);

	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		KvStateInfo<?, ?, ?> stateInfo = (KvStateInfo<?, ?, ?>) o;
		return Objects.equals(keySerializer, stateInfo.keySerializer) &&
				Objects.equals(namespaceSerializer, stateInfo.namespaceSerializer) &&
				Objects.equals(stateValueSerializer, stateInfo.stateValueSerializer);
	}

	@Override
	public int hashCode() {
		return Objects.hash(keySerializer, namespaceSerializer, stateValueSerializer);
	}
}
