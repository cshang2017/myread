package org.apache.flink.table.runtime.dataview;

import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * An {@link Iterator} implementation that iterate on a map iterator and a null entry.
 */
public class NullAwareMapIterator<K, V> implements Iterator<Map.Entry<K, V>> {

	private final Iterator<Map.Entry<K, V>> mapIterator;
	private final NullMapEntry<K, V> nullMapEntry;
	private final boolean nullExisted;
	private boolean nullVisited = false;

	public NullAwareMapIterator(Iterator<Map.Entry<K, V>> mapIterator, NullMapEntry<K, V> nullMapEntry) {
		this.mapIterator = mapIterator;
		this.nullMapEntry = nullMapEntry;
		this.nullExisted = nullMapEntry.getValue() != null;
	}

	@Override
	public boolean hasNext() {
		return mapIterator.hasNext() || (nullExisted && !nullVisited);
	}

	@Override
	public Map.Entry<K, V> next() {
		if (mapIterator.hasNext()) {
			return mapIterator.next();
		} else if (nullExisted && !nullVisited) {
			this.nullVisited = true;
			return nullMapEntry;
		} else {
			throw new NoSuchElementException();
		}
	}

	@Override
	public void remove() {
		if (nullExisted && nullVisited) {
			nullMapEntry.remove();
		} else {
			mapIterator.remove();
		}
	}

	/**
	 * A Map Entry that the entry key is always null.
	 */
	public interface NullMapEntry<K, V> extends Map.Entry<K, V> {

		@Override
		default K getKey() {
			// the key is always null
			return null;
		}

		void remove();
	}
}
