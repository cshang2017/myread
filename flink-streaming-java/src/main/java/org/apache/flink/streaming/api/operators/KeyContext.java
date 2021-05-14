package org.apache.flink.streaming.api.operators;

/**
 * Inteface for setting and querying the current key of keyed operations.
 *
 * <p>This is mainly used by the timer system to query the key when creating timers
 * and to set the correct key context when firing a timer.
 */
public interface KeyContext {

	void setCurrentKey(Object key);

	Object getCurrentKey();
}
