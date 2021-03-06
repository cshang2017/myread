package org.apache.flink.api.common.time;

import org.apache.flink.annotation.Internal;

import java.time.Duration;
import java.util.concurrent.TimeoutException;

/**
 * This class stores a deadline, as obtained via {@link #now()} or from {@link #plus(Duration)}.
 */
@Internal
public class Deadline {

	/** The deadline, relative to {@link System#nanoTime()}. */
	private final long timeNanos;

	private Deadline(long deadline) {
		this.timeNanos = deadline;
	}

	public Deadline plus(Duration other) {
		return new Deadline(Math.addExact(timeNanos, other.toNanos()));
	}

	/**
	 * Returns the time left between the deadline and now. The result is negative if the deadline
	 * has passed.
	 */
	public Duration timeLeft() {
		return Duration.ofNanos(Math.subtractExact(timeNanos, System.nanoTime()));
	}

	/**
	 * Returns the time left between the deadline and now. If no time is left, a {@link TimeoutException} will be thrown.
	 *
	 * @throws TimeoutException if no time is left
	 */
	public Duration timeLeftIfAny() throws TimeoutException {
		long nanos = Math.subtractExact(timeNanos, System.nanoTime());
		if (nanos <= 0) {
			throw new TimeoutException();
		}
		return Duration.ofNanos(nanos);
	}

	/**
	 * Returns whether there is any time left between the deadline and now.
	 */
	public boolean hasTimeLeft() {
		return !isOverdue();
	}

	/**
	 * Determines whether the deadline is in the past, i.e. whether the time left is negative.
	 */
	public boolean isOverdue() {
		return timeNanos < System.nanoTime();
	}

	// ------------------------------------------------------------------------
	//  Creating Deadlines
	// ------------------------------------------------------------------------

	/**
	 * Constructs a {@link Deadline} that has now as the deadline. Use this and then extend via
	 * {@link #plus(Duration)} to specify a deadline in the future.
	 */
	public static Deadline now() {
		return new Deadline(System.nanoTime());
	}

	/**
	 * Constructs a Deadline that is a given duration after now.
	 */
	public static Deadline fromNow(Duration duration) {
		return new Deadline(Math.addExact(System.nanoTime(), duration.toNanos()));
	}
}
