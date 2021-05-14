
package org.apache.flink.table.planner.utils;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Copied from {@link org.apache.flink.runtime.util.SingleElementIterator}.
 * Fix iterator to set available true.
 */
public final class SingleElementIterator<E> implements Iterator<E>, Iterable<E> {

	private E current;
	private boolean available = false;

	/**
	 * Resets the element. After this call, the iterator has one element available, which is the
	 * given element.
	 *
	 * @param current The element to make available to the iterator.
	 */
	public void set(E current) {
		this.current = current;
		this.available = true;
	}

	@Override
	public boolean hasNext() {
		return available;
	}

	@Override
	public E next() {
		if (available) {
			available = false;
			return current;
		} else {
			throw new NoSuchElementException();
		}
	}

	@Override
	public void remove() {
		throw new UnsupportedOperationException();
	}

	@Override
	public Iterator<E> iterator() {
		available = true;
		return this;
	}
}
