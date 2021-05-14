
package org.apache.flink.table.utils;

import org.apache.flink.annotation.Internal;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Provides a thread local cache with a maximum cache size per thread.
 *
 * <p>Note: Values must not be null.
 */
@Internal
public abstract class ThreadLocalCache<K, V> {

	private static final int DEFAULT_CACHE_SIZE = 64;

	private final ThreadLocal<BoundedMap<K, V>> cache = new ThreadLocal<>();
	private final int maxSizePerThread;

	protected ThreadLocalCache() {
		this(DEFAULT_CACHE_SIZE);
	}

	protected ThreadLocalCache(int maxSizePerThread) {
		this.maxSizePerThread = maxSizePerThread;
	}

	public V get(K key) {
		BoundedMap<K, V> map = cache.get();
		if (map == null) {
			map = new BoundedMap<>(maxSizePerThread);
			cache.set(map);
		}
		V value = map.get(key);
		if (value == null) {
			value = getNewInstance(key);
			map.put(key, value);
		}
		return value;
	}

	public abstract V getNewInstance(K key);

	private static class BoundedMap<K, V> extends LinkedHashMap<K, V> {

		private final int maxSize;

		private BoundedMap(int maxSize) {
			this.maxSize = maxSize;
		}

		@Override
		protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
			return this.size() > maxSize;
		}
	}
}
