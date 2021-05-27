package org.apache.flink.runtime.state;

/**
 * Interface of entries in a state backend. Entries are triple of key, namespace, and state.
 *
 * @param <K> type of key.
 * @param <N> type of namespace.
 * @param <S> type of state.
 */
public interface StateEntry<K, N, S> {

	/**
	 * Returns the key of this entry.
	 */
	K getKey();

	/**
	 * Returns the namespace of this entry.
	 */
	N getNamespace();

	/**
	 * Returns the state of this entry.
	 */
	S getState();

	class SimpleStateEntry<K, N, S> implements StateEntry<K, N, S> {
		private final K key;
		private final N namespace;
		private final S value;

		public SimpleStateEntry(K key, N namespace, S value) {
			this.key = key;
			this.namespace = namespace;
			this.value = value;
		}

		@Override
		public K getKey() {
			return key;
		}

		@Override
		public N getNamespace() {
			return namespace;
		}

		@Override
		public S getState() {
			return value;
		}
	}
}
