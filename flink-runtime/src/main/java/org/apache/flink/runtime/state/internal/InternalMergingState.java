

package org.apache.flink.runtime.state.internal;

import org.apache.flink.api.common.state.MergingState;

import java.util.Collection;

/**
 * The peer to the {@link MergingState} in the internal state type hierarchy.
 * 
 * See {@link InternalKvState} for a description of the internal state hierarchy.
 *
 * @param <K> The type of key the state is associated to
 * @param <N> The type of the namespace
 * @param <IN> The type of elements added to the state
 * @param <SV> The type of elements in the state
 * @param <OUT> The type of elements
 */
public interface InternalMergingState<K, N, IN, SV, OUT> extends InternalAppendingState<K, N, IN, SV, OUT>, MergingState<IN, OUT> {

	/**
	 * Merges the state of the current key for the given source namespaces into the state of
	 * the target namespace.
	 * 
	 * @param target The target namespace where the merged state should be stored.
	 * @param sources The source namespaces whose state should be merged.
	 * 
	 * @throws Exception The method may forward exception thrown internally (by I/O or functions).
	 */
	void mergeNamespaces(N target, Collection<N> sources) throws Exception;
}
