package org.apache.flink.api.connector.source;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.core.io.InputStatus;

import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * The interface for a source reader which is responsible for reading the records from
 * the source splits assigned by {@link SplitEnumerator}.
 *
 * @param <T> The type of the record emitted by this source reader.
 * @param <SplitT> The type of the the source splits.
 */
@PublicEvolving
public interface SourceReader<T, SplitT extends SourceSplit>
		extends AutoCloseable {

	/**
	 * Start the reader.
	 */
	void start();

	/**
	 * Poll the next available record into the {@link SourceOutput}.
	 *
	 * <p>The implementation must make sure this method is non-blocking.
	 *
	 * <p>Although the implementation can emit multiple records into the given SourceOutput,
	 * it is recommended not doing so. Instead, emit one record into the SourceOutput
	 * and return a {@link InputStatus#MORE_AVAILABLE} to let the caller thread
	 * know there are more records available.
	 *
	 * @return The InputStatus of the SourceReader after the method invocation.
	 */
	InputStatus pollNext(ReaderOutput<T> output) throws Exception;

	/**
	 * Checkpoint on the state of the source.
	 *
	 * @return the state of the source.
	 */
	List<SplitT> snapshotState(long checkpointId);

	/**
	 * @return a future that will be completed once there is a record available to poll.
	 */
	CompletableFuture<Void> isAvailable();

	/**
	 * Adds a list of splits for this reader to read.
	 *
	 * @param splits The splits assigned by the split enumerator.
	 */
	void addSplits(List<SplitT> splits);

	/**
	 * This method is called when the reader is notified that it will not
	 * receive any further splits.
	 *
	 * <p>It is triggered when the enumerator calls {@link SplitEnumeratorContext#signalNoMoreSplits(int)}
	 * with the reader's parallel subtask.
	 */
	void notifyNoMoreSplits();

	/**
	 * Handle a source event sent by the {@link SplitEnumerator}.
	 *
	 * <p>This method has a default implementation that does nothing, because
	 * most sources do not require any custom events.
	 *
	 * @param sourceEvent the event sent by the {@link SplitEnumerator}.
	 */
	default void handleSourceEvents(SourceEvent sourceEvent) {}

	/**
	 * We have an empty default implementation here because most source readers do not have
	 * to implement the method.
	 */
	default void notifyCheckpointComplete(long checkpointId) throws Exception {}

	/**
	 * Called when a checkpoint is aborted.
	 *
	 * <p>NOTE: This method is here only in the backport to the Flink 1.11 branch.
	 * In 1.12, this default method is inherited from the CheckpointListener interface.
	 */
	default void notifyCheckpointAborted(long checkpointId) {}
}
