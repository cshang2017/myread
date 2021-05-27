package org.apache.flink.runtime.operators.util;

import java.io.IOException;

import org.apache.flink.runtime.memory.MemoryAllocationException;

/**
 * Interface describing the methods that have to be implemented by local strategies for the CoGroup Pact.
 * 
 * @param <T1> The generic type of the first input's data type.
 * @param <T2> The generic type of the second input's data type.
 */
public interface CoGroupTaskIterator<T1, T2> {
	
	/**
	 * General-purpose open method.
	 * 
	 * @throws IOException
	 * @throws MemoryAllocationException
	 * @throws InterruptedException 
	 */
	void open() throws IOException, MemoryAllocationException, InterruptedException;

	/**
	 * General-purpose close method.
	 */
	void close();

	/**
	 * Moves the internal pointer to the next key (if present). Returns true if the operation was
	 * successful or false if no more keys are present.
	 * <p>
	 * The key is not necessarily shared by both inputs. In that case an empty iterator is 
	 * returned by getValues1() or getValues2().
	 * 
	 * @return true on success, false if no more keys are present
	 * @throws IOException
	 */
	boolean next() throws IOException;

	/**
	 * Returns an iterable over the left input values for the current key.
	 * 
	 * @return an iterable over the left input values for the current key.
	 */
	Iterable<T1> getValues1();

	/**
	 * Returns an iterable over the left input values for the current key.
	 * 
	 * @return an iterable over the left input values for the current key.
	 */
	Iterable<T2> getValues2();
}
