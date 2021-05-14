package org.apache.flink.streaming.api.operators.async.queue;

import org.apache.flink.annotation.Internal;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.operators.TimestampedCollector;
import org.apache.flink.streaming.api.operators.async.AsyncWaitOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;

import java.util.List;
import java.util.Optional;

/**
 * Interface for stream element queues for the {@link AsyncWaitOperator}.
 */
@Internal
public interface StreamElementQueue<OUT> {

	/**
	 * Tries to put the given element in the queue. This operation succeeds if the queue has capacity left and fails if
	 * the queue is full.
	 *
	 * <p>This method returns a handle to the inserted element that allows to set the result of the computation.
	 *
	 * @param streamElement the element to be inserted.
	 * @return A handle to the element if successful or {@link Optional#empty()} otherwise.
	 */
	Optional<ResultFuture<OUT>> tryPut(StreamElement streamElement);

	/**
	 * Emits one completed element from the head of this queue into the given output.
	 *
	 * <p>Will not emit any element if no element has been completed (check {@link #hasCompletedElements()} before entering
	 * any critical section).
	 *
	 * @param output the output into which to emit
	 */
	void emitCompletedElement(TimestampedCollector<OUT> output);

	/**
	 * Checks if there is at least one completed head element.
	 *
	 * @return True if there is a completed head element.
	 */
	boolean hasCompletedElements();

	/**
	 * Returns the collection of {@link StreamElement} currently contained in this queue for checkpointing.
	 *
	 * <p>This includes all non-emitted, completed and non-completed elements.
	 *
	 * @return List of currently contained {@link StreamElement}.
	 */
	List<StreamElement> values();

	/**
	 * True if the queue is empty; otherwise false.
	 *
	 * @return True if the queue is empty; otherwise false.
	 */
	boolean isEmpty();

	/**
	 * Return the size of the queue.
	 *
	 * @return The number of elements contained in this queue.
	 */
	int size();
}
