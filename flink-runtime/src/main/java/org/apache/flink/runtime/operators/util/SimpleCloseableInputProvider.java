package org.apache.flink.runtime.operators.util;

import java.io.IOException;

import org.apache.flink.util.MutableObjectIterator;

/**
 * A simple iterator provider that returns a supplied iterator and does nothing when closed.
 */
public class SimpleCloseableInputProvider<E> implements CloseableInputProvider<E>
{
	/**
	 * The iterator returned by this class.
	 */
	private final MutableObjectIterator<E> iterator;

	/**
	 * Creates a new simple input provider that will return the given iterator.
	 * 
	 * @param iterator The iterator that will be returned.
	 */
	public SimpleCloseableInputProvider(MutableObjectIterator<E> iterator) {
		this.iterator = iterator;
	}

	@Override
	public void close() throws IOException {
		// do nothing
	}

	@Override
	public MutableObjectIterator<E> getIterator() {
		return this.iterator;
	}

}
