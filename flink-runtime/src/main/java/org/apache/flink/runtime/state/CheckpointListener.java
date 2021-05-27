package org.apache.flink.runtime.state;

import org.apache.flink.annotation.Public;

/**
 * This interface must be implemented by functions/operations that want to receive
 * a commit notification once a checkpoint has been completely acknowledged by all
 * participants.
 */
@Public
public interface CheckpointListener {

	/**
	 * This method is called as a notification once a distributed checkpoint has been completed.
	 *
	 * <p>Note that any exception during this method will not cause the checkpoint to
	 * fail any more.
	 *
	 * @param checkpointId The ID of the checkpoint that has been completed.
	 * @throws Exception This method can propagate exceptions, which leads to a failure/recovery for
	 *                   the task. Not that this will NOT lead to the checkpoint being revoked.
	 */
	void notifyCheckpointComplete(long checkpointId) throws Exception;

	/**
	 * This method is called as a notification once a distributed checkpoint has been aborted.
	 *
	 * @param checkpointId The ID of the checkpoint that has been aborted.
	 * @throws Exception This method can propagate exceptions, which leads to a failure/recovery for
	 *                   the task.
	 */
	default void notifyCheckpointAborted(long checkpointId) throws Exception {}
}
