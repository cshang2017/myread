package org.apache.flink.streaming.runtime.tasks;

import org.apache.flink.annotation.Internal;

/**
 * A common timer service interface with life cycle methods.
 *
 * <p>The registration of timers follows a life cycle of three phases:
 * <ol>
 *     <li>In the initial state, it accepts timer registrations and triggers when the time is reached.</li>
 *     <li>After calling {@link #quiesce()}, further calls to
 *         {@link #registerTimer(long, ProcessingTimeCallback)} will not register any further timers, and will
 *         return a "dummy" future as a result. This is used for clean shutdown, where currently firing
 *         timers are waited for and no future timers can be scheduled, without causing hard exceptions.</li>
 *     <li>After a call to {@link #shutdownService()}, all calls to {@link #registerTimer(long, ProcessingTimeCallback)}
 *         will result in a hard exception.</li>
 * </ol>
 */
@Internal
public interface TimerService extends ProcessingTimeService {

	/**
	 * Returns <tt>true</tt> if the service has been shut down, <tt>false</tt> otherwise.
	 */
	boolean isTerminated();

	/**
	 * Shuts down and clean up the timer service provider hard and immediately. This does not wait
	 * for any timer to complete. Any further call to {@link #registerTimer(long, ProcessingTimeCallback)}
	 * will result in a hard exception.
	 */
	void shutdownService();

	/**
	 * Shuts down and clean up the timer service provider hard and immediately. This does not wait
	 * for any timer to complete. Any further call to {@link #registerTimer(long, ProcessingTimeCallback)}
	 * will result in a hard exception. This call cannot be interrupted and will block until the shutdown is completed
	 * or the timeout is exceeded.
	 *
	 * @param timeoutMs timeout for blocking on the service shutdown in milliseconds.
	 * @return returns true iff the shutdown was completed.
	 */
	boolean shutdownServiceUninterruptible(long timeoutMs);
}
