package org.apache.flink.api.connector.source.lib.util;

import org.apache.flink.api.connector.source.SourceSplit;

import java.util.Iterator;

/**
 * A {@link SourceSplit} that represents a sequence of elements captured in an iterator.
 * The split produces the iterator and converts the iterator back to a new split during
 * checkpointing.
 *
 * @param <E> The type of the elements returned by the iterator.
 */
public interface IteratorSourceSplit<E, IterT extends Iterator<E>> extends SourceSplit {

	/**
	 * Gets the iterator over the elements of this split.
	 */
	IterT getIterator();

	/**
	 * Converts an iterator (that may have returned some elements already) back into a source split.
	 */
	IteratorSourceSplit<E, IterT> getUpdatedSplitForIterator(IterT iterator);
}
