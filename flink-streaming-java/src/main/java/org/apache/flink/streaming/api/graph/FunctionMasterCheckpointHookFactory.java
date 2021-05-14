package org.apache.flink.streaming.api.graph;

import org.apache.flink.runtime.checkpoint.MasterTriggerRestoreHook;
import org.apache.flink.streaming.api.checkpoint.WithMasterCheckpointHook;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Utility class that turns a {@link WithMasterCheckpointHook} into a
 * {@link org.apache.flink.runtime.checkpoint.MasterTriggerRestoreHook.Factory}.
 */
class FunctionMasterCheckpointHookFactory implements MasterTriggerRestoreHook.Factory {

	private final WithMasterCheckpointHook<?> creator;

	FunctionMasterCheckpointHookFactory(WithMasterCheckpointHook<?> creator) {
		this.creator = checkNotNull(creator);
	}

	@SuppressWarnings("unchecked")
	@Override
	public <V> MasterTriggerRestoreHook<V> create() {
		return (MasterTriggerRestoreHook<V>) creator.createMasterTriggerRestoreHook();
	}
}
