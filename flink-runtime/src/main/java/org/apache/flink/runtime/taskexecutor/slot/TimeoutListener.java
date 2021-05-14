package org.apache.flink.runtime.taskexecutor.slot;

import java.util.UUID;

/**
 * Listener for timeout events by the {@link TimerService}.
 * @param <K> Type of the timeout key
 */
public interface TimeoutListener<K> {

	/**
	 * Notify the listener about the timeout for an event identified by key. Additionally the method
	 * is called with the timeout ticket which allows to identify outdated timeout events.
	 *
	 * @param key identifying the timed out event
	 * @param ticket used to check whether the timeout is still valid
	 */
	void notifyTimeout(K key, UUID ticket);
}
