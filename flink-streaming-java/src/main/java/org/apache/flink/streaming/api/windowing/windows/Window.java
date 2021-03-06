package org.apache.flink.streaming.api.windowing.windows;

import org.apache.flink.annotation.PublicEvolving;

/**
 * A {@code Window} is a grouping of elements into finite buckets. Windows have a maximum timestamp
 * which means that, at some point, all elements that go into one window will have arrived.
 *
 * <p>Subclasses should implement {@code equals()} and {@code hashCode()} so that logically
 * same windows are treated the same.
 */
@PublicEvolving
public abstract class Window {

	/**
	 * Gets the largest timestamp that still belongs to this window.
	 *
	 * @return The largest timestamp that still belongs to this window.
	 */
	public abstract long maxTimestamp();
}
