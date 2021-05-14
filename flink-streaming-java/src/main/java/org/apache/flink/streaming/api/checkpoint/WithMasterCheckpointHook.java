package org.apache.flink.streaming.api.checkpoint;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.runtime.checkpoint.MasterTriggerRestoreHook;

/**
 * This interface can be implemented by streaming functions that need to trigger a
 * "global action" on the master (in the checkpoint coordinator) as part of every
 * checkpoint and restore operation.
 *
 * @param <E> The type of the data stored by the hook in the checkpoint, or {@code Void}, if none.
 */
@PublicEvolving
public interface WithMasterCheckpointHook<E> extends java.io.Serializable {

	/**
	 * Creates the hook that should be called by the checkpoint coordinator.
	 */
	MasterTriggerRestoreHook<E> createMasterTriggerRestoreHook();
}
