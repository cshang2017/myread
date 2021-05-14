package org.apache.flink.streaming.api.operators.async.queue;

import org.apache.flink.annotation.Internal;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.operators.TimestampedCollector;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;

import javax.annotation.Nonnull;

/**
 * An entry for the {@link StreamElementQueue}. The stream element queue entry stores the {@link StreamElement} for
 * which the stream element queue entry has been instantiated.
 * Furthermore, it allows to set the result of a completed entry through {@link ResultFuture}.
 */
@Internal
interface StreamElementQueueEntry<OUT> extends ResultFuture<OUT> {

	/**
	 * True if the stream element queue entry has been completed; otherwise false.
	 *
	 * @return True if the stream element queue entry has been completed; otherwise false.
	 */
	boolean isDone();

	/**
	 * Emits the results associated with this queue entry.
	 *
	 * @param output the output into which to emit.
	 */
	void emitResult(TimestampedCollector<OUT> output);

	/**
	 * The input element for this queue entry, for which the calculation is performed asynchronously.
	 *
	 * @return the input element.
	 */
	@Nonnull StreamElement getInputElement();

	/**
	 * Not supported. Exceptions must be handled in the AsyncWaitOperator.
	 */
	@Override
	default void completeExceptionally(Throwable error) {
		throw new UnsupportedOperationException("This result future should only be used to set completed results.");
	}
}
