package org.apache.flink.runtime.jobmaster.slotpool;

import org.apache.flink.api.java.tuple.Tuple2;

import java.util.AbstractCollection;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Set;

/**
 * Map which stores values under two different indices.
 *
 * @param <A> Type of key A
 * @param <B> Type of key B
 * @param <V> Type of the value
 */
public class DualKeyLinkedMap<A, B, V> {

	private final LinkedHashMap<A, Tuple2<B, V>> aMap;

	private final LinkedHashMap<B, A> bMap;

	private transient Collection<V> values;

	public DualKeyLinkedMap(int initialCapacity) {
		this.aMap = new LinkedHashMap<>(initialCapacity);
		this.bMap = new LinkedHashMap<>(initialCapacity);
	}

	public int size() {
		return aMap.size();
	}

	public V getKeyA(A aKey) {
		final Tuple2<B, V> value = aMap.get(aKey);

		if (value != null) {
			return value.f1;
		} else {
			return null;
		}
	}

	public V getKeyB(B bKey) {
		final A aKey = bMap.get(bKey);

		if (aKey != null) {
			return aMap.get(aKey).f1;
		} else {
			return null;
		}
	}

	public V put(A aKey, B bKey, V value) {
		final V removedValue = removeKeyA(aKey);
		removeKeyB(bKey);

		aMap.put(aKey, Tuple2.of(bKey, value));
		bMap.put(bKey, aKey);

		if (removedValue != null) {
			return removedValue;
		} else {
			return null;
		}
	}

	public boolean containsKeyA(A aKey) {
		return aMap.containsKey(aKey);
	}

	public boolean containsKeyB(B bKey) {
		return bMap.containsKey(bKey);
	}

	public V removeKeyA(A aKey) {
		Tuple2<B, V> aValue = aMap.remove(aKey);

		if (aValue != null) {
			bMap.remove(aValue.f0);
			return aValue.f1;
		} else {
			return null;
		}
	}

	public V removeKeyB(B bKey) {
		A aKey = bMap.remove(bKey);

		if (aKey != null) {
			Tuple2<B, V> aValue = aMap.remove(aKey);
			if (aValue != null) {
				return aValue.f1;
			} else {
				return null;
			}
		} else {
			return null;
		}
	}

	public Collection<V> values() {
		Collection<V> vs = values;

		if (vs == null) {
			vs = new Values();
			values = vs;
		}

		return vs;
	}

	public Set<A> keySetA() {
		return aMap.keySet();
	}

	public Set<B> keySetB() {
		return bMap.keySet();
	}

	public void clear() {
		aMap.clear();
		bMap.clear();
	}

	// -----------------------------------------------------------------------
	// Inner classes
	// -----------------------------------------------------------------------

	/**
	 * Collection which contains the values of the dual key map.
	 */
	private final class Values extends AbstractCollection<V> {

		@Override
		public Iterator<V> iterator() {
			return new ValueIterator();
		}

		@Override
		public int size() {
			return aMap.size();
		}
	}

	/**
	 * Iterator which iterates over the values of the dual key map.
	 */
	private final class ValueIterator implements Iterator<V> {

		private final Iterator<Tuple2<B, V>> iterator = aMap.values().iterator();

		@Override
		public boolean hasNext() {
			return iterator.hasNext();
		}

		@Override
		public V next() {
			Tuple2<B, V> value = iterator.next();

			return value.f1;
		}
	}
}
