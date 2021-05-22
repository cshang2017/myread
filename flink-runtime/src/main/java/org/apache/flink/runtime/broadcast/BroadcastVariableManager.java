package org.apache.flink.runtime.broadcast;

import org.apache.flink.api.common.typeutils.TypeSerializerFactory;
import org.apache.flink.runtime.io.network.api.reader.MutableReader;
import org.apache.flink.runtime.operators.BatchTask;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The BroadcastVariableManager is used to manage the materialization of broadcast variables. References to materialized
 * broadcast variables are cached and shared between parallel subtasks. A reference count is maintained to track whether
 * the materialization may be cleaned up.
 */
public class BroadcastVariableManager {

	private final ConcurrentHashMap<BroadcastVariableKey, BroadcastVariableMaterialization<?, ?>> variables =
							new ConcurrentHashMap<BroadcastVariableKey, BroadcastVariableMaterialization<?, ?>>(16);

	// --------------------------------------------------------------------------------------------

	/**
	 * Materializes the broadcast variable for the given name, scoped to the given task and its iteration superstep. An
	 * existing materialization created by another parallel subtask may be returned, if it hasn't expired yet.
	 */
	public <T> BroadcastVariableMaterialization<T, ?> materializeBroadcastVariable(String name, int superstep, BatchTask<?, ?> holder,
			MutableReader<?> reader, TypeSerializerFactory<T> serializerFactory) throws IOException {
		final BroadcastVariableKey key = new BroadcastVariableKey(holder.getEnvironment().getJobVertexId(), name, superstep);

		while (true) {
			final BroadcastVariableMaterialization<T, Object> newMat = new BroadcastVariableMaterialization<T, Object>(key);

			final BroadcastVariableMaterialization<?, ?> previous = variables.putIfAbsent(key, newMat);

			@SuppressWarnings("unchecked")
			final BroadcastVariableMaterialization<T, ?> materialization = (previous == null) ? newMat : (BroadcastVariableMaterialization<T, ?>) previous;

				materialization.materializeVariable(reader, serializerFactory, holder);
				return materialization;
		}
	}

	public void releaseReference(String name, int superstep, BatchTask<?, ?> referenceHolder) {
		BroadcastVariableKey key = new BroadcastVariableKey(referenceHolder.getEnvironment().getJobVertexId(), name, superstep);
		releaseReference(key, referenceHolder);
	}

	public void releaseReference(BroadcastVariableKey key, BatchTask<?, ?> referenceHolder) {
		BroadcastVariableMaterialization<?, ?> mat = variables.get(key);

		// release this reference
		if (mat.decrementReference(referenceHolder)) {
			// remove if no one holds a reference and no one concurrently replaced the entry
			variables.remove(key, mat);
		}
	}

	public void releaseAllReferencesFromTask(BatchTask<?, ?> referenceHolder) {
		// go through all registered variables
		for (Map.Entry<BroadcastVariableKey, BroadcastVariableMaterialization<?, ?>> entry : variables.entrySet()) {
			BroadcastVariableMaterialization<?, ?> mat = entry.getValue();

			// release the reference
			if (mat.decrementReferenceIfHeld(referenceHolder)) {
				// remove if no one holds a reference and no one concurrently replaced the entry
				variables.remove(entry.getKey(), mat);
			}
		}
	}

	// --------------------------------------------------------------------------------------------

	public int getNumberOfVariablesWithReferences() {
		return this.variables.size();
	}
}
