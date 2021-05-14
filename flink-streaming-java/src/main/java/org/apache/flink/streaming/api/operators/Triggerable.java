package org.apache.flink.streaming.api.operators;

import org.apache.flink.annotation.Internal;

/**
 * Interface for things that can be called by {@link InternalTimerService}.
 *
 * @param <K> Type of the keys to which timers are scoped.
 * @param <N> Type of the namespace to which timers are scoped.
 */
@Internal
public interface Triggerable<K, N> {

	/**
	 * Invoked when an event-time timer fires.
	 */
	void onEventTime(InternalTimer<K, N> timer) throws Exception;

	/**
	 * Invoked when a processing-time timer fires.
	 */
	void onProcessingTime(InternalTimer<K, N> timer) throws Exception;
}
