

package org.apache.flink.runtime.state;

/**
 * Interface for objects that have a key attribute.
 *
 * @param <K> type of the key.
 */
public interface Keyed<K> {

	/**
	 * Returns the key attribute.
	 */
	K getKey();
}
