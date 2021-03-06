package org.apache.flink.streaming.runtime.tasks;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * Defines the current processing time and handles all related actions,
 * such as register timers for tasks to be executed in the future.
 *
 * <p>The access to the time via {@link #getCurrentProcessingTime()} is always available, regardless of
 * whether the timer service has been shut down.
 */
public interface ProcessingTimeService {

	/**
	 * Returns the current processing time.
	 */
	long getCurrentProcessingTime();

	/**
	 * Registers a task to be executed when (processing) time is {@code timestamp}.
	 *
	 * @param timestamp   Time when the task is to be executed (in processing time)
	 * @param target      The task to be executed
	 *
	 * @return The future that represents the scheduled task. This always returns some future,
	 *         even if the timer was shut down
	 */
	ScheduledFuture<?> registerTimer(long timestamp, ProcessingTimeCallback target);

	/**
	 * Registers a task to be executed repeatedly at a fixed rate.
	 *
	 * <p>This call behaves similar to
	 * {@link org.apache.flink.runtime.concurrent.ScheduledExecutor#scheduleAtFixedRate(Runnable, long, long, TimeUnit)}.
	 *
	 * @param callback to be executed after the initial delay and then after each period
	 * @param initialDelay initial delay to start executing callback
	 * @param period after the initial delay after which the callback is executed
	 * @return Scheduled future representing the task to be executed repeatedly
	 */
	ScheduledFuture<?> scheduleAtFixedRate(ProcessingTimeCallback callback, long initialDelay, long period);

	/**
	 * Registers a task to be executed repeatedly with a fixed delay.
	 *
	 * <p>This call behaves similar to
	 * {@link org.apache.flink.runtime.concurrent.ScheduledExecutor#scheduleWithFixedDelay(Runnable, long, long, TimeUnit)}.
	 *
	 * @param callback to be executed after the initial delay and then after each period
	 * @param initialDelay initial delay to start executing callback
	 * @param period after the initial delay after which the callback is executed
	 * @return Scheduled future representing the task to be executed repeatedly
	 */
	ScheduledFuture<?> scheduleWithFixedDelay(ProcessingTimeCallback callback, long initialDelay, long period);

	/**
	 * This method puts the service into a state where it does not register new timers, but
	 * returns for each call to {@link #registerTimer} or {@link #scheduleAtFixedRate} a "mock"
	 * future and the "mock" future will be never completed. Furthermore, the timers registered
	 * before are prevented from firing, but the timers in running are allowed to finish.
	 *
	 * <p>If no timer is running, the quiesce-completed future is immediately completed and
	 * returned. Otherwise, the future returned will be completed when all running timers have
	 * finished.
	 */
	CompletableFuture<Void> quiesce();
}
