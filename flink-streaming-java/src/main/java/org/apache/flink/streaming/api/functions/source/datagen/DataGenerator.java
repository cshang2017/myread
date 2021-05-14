package org.apache.flink.streaming.api.functions.source.datagen;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;

import java.io.Serializable;
import java.util.Iterator;

/**
 * Stateful and re-scalable data generator.
 */
@Experimental
public interface DataGenerator<T> extends Serializable, Iterator<T> {

	/**
	 * Open and initialize state for {@link DataGenerator}.
	 * See {@link CheckpointedFunction#initializeState}.
	 *
	 * @param name The state of {@link DataGenerator} should related to this name, make sure
	 *             the name of state is different.
	 */
	void open(
			String name,
			FunctionInitializationContext context,
			RuntimeContext runtimeContext) throws Exception;

	/**
	 * Snapshot state for {@link DataGenerator}.
	 * See {@link CheckpointedFunction#snapshotState}.
	 */
	default void snapshotState(FunctionSnapshotContext context) throws Exception {}
}
