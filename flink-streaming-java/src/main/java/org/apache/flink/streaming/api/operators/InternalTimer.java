package org.apache.flink.streaming.api.operators;

import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.state.KeyExtractorFunction;
import org.apache.flink.runtime.state.Keyed;
import org.apache.flink.runtime.state.PriorityComparable;
import org.apache.flink.runtime.state.PriorityComparator;

import javax.annotation.Nonnull;

/**
 * Internal interface for in-flight timers.
 *
 * @param <K> Type of the keys to which timers are scoped.
 * @param <N> Type of the namespace to which timers are scoped.
 */
@Internal
public interface InternalTimer<K, N> extends PriorityComparable<InternalTimer<?, ?>>, Keyed<K> {

	/** Function to extract the key from a {@link InternalTimer}. */
	KeyExtractorFunction<InternalTimer<?, ?>> KEY_EXTRACTOR_FUNCTION = InternalTimer::getKey;

	/** Function to compare instances of {@link InternalTimer}. */
	PriorityComparator<InternalTimer<?, ?>> TIMER_COMPARATOR =
		(left, right) -> Long.compare(left.getTimestamp(), right.getTimestamp());
	/**
	 * Returns the timestamp of the timer. This value determines the point in time when the timer will fire.
	 */
	long getTimestamp();

	/**
	 * Returns the key that is bound to this timer.
	 */
	@Nonnull
	@Override
	K getKey();

	/**
	 * Returns the namespace that is bound to this timer.
	 */
	@Nonnull
	N getNamespace();
}
