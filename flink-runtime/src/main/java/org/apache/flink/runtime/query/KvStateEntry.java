
package org.apache.flink.runtime.query;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.state.internal.InternalKvState;
import org.apache.flink.util.Preconditions;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * An entry holding the {@link InternalKvState} along with its {@link KvStateInfo}.
 *
 * @param <K> The type of key the state is associated to
 * @param <N> The type of the namespace the state is associated to
 * @param <V> The type of values kept internally in state
 */
@Internal
public class KvStateEntry<K, N, V> {

	private final InternalKvState<K, N, V> state;
	private final KvStateInfo<K, N, V> stateInfo;

	private final boolean areSerializersStateless;

	private final ConcurrentMap<Thread, KvStateInfo<K, N, V>> serializerCache;

	public KvStateEntry(final InternalKvState<K, N, V> state) {
		this.state = Preconditions.checkNotNull(state);
		this.stateInfo = new KvStateInfo<>(
				state.getKeySerializer(),
				state.getNamespaceSerializer(),
				state.getValueSerializer()
		);
		this.serializerCache = new ConcurrentHashMap<>();
		this.areSerializersStateless = stateInfo.duplicate() == stateInfo;
	}

	public InternalKvState<K, N, V> getState() {
		return state;
	}

	public KvStateInfo<K, N, V> getInfoForCurrentThread() {
		return areSerializersStateless
				? stateInfo
				: serializerCache.computeIfAbsent(Thread.currentThread(), t -> stateInfo.duplicate());
	}

	public void clear() {
		serializerCache.clear();
	}

	@VisibleForTesting
	public int getCacheSize() {
		return serializerCache.size();
	}
}
