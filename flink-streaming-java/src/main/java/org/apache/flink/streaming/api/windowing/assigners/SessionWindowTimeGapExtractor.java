package org.apache.flink.streaming.api.windowing.assigners;

import org.apache.flink.annotation.PublicEvolving;

import java.io.Serializable;

/**
 * A {@code SessionWindowTimeGapExtractor} extracts session time gaps for Dynamic Session Window Assigners.
 *
 * @param <T> The type of elements that this {@code SessionWindowTimeGapExtractor} can extract session time gaps from.
 */
@PublicEvolving
public interface SessionWindowTimeGapExtractor<T> extends Serializable {
	/**
	 * Extracts the session time gap.
	 * @param element The input element.
	 * @return The session time gap in milliseconds.
	 */
	long extract(T element);
}
