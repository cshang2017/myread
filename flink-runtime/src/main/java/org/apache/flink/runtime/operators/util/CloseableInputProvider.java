package org.apache.flink.runtime.operators.util;

import java.io.Closeable;
import java.io.IOException;

import org.apache.flink.util.MutableObjectIterator;

/**
 * Utility interface for a provider of an input that can be closed.
 */
public interface CloseableInputProvider<E> extends Closeable
{
	/**
	 * Gets the iterator over this input.
	 * 
	 * @return The iterator provided by this iterator provider.
	 * @throws InterruptedException 
	 */
	public MutableObjectIterator<E> getIterator() throws InterruptedException, IOException;
}
