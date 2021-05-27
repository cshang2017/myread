package org.apache.flink.runtime.state.internal;

import org.apache.flink.api.common.state.AppendingState;

/**
 * The peer to the {@link AppendingState} in the internal state type hierarchy.
 *
 * <p>See {@link InternalKvState} for a description of the internal state hierarchy.
 *
 * @param <K> The type of key the state is associated to
 * @param <N> The type of the namespace
 * @param <IN> The type of elements added to the state
 * @param <SV> The type of elements in the state
 * @param <OUT> The type of the resulting element in the state
 */
public interface InternalAppendingState<K, N, IN, SV, OUT> extends InternalKvState<K, N, SV>, AppendingState<IN, OUT> {
	/**
	 * Get internally stored value.
	 *
	 * @return internally stored value.
	 *
	 * @throws Exception The method may forward exception thrown internally (by I/O or functions).
	 */
	SV getInternal() throws Exception;

	/**
	 * Update internally stored value.
	 *
	 * @param valueToStore new value to store.
	 *
	 * @throws Exception The method may forward exception thrown internally (by I/O or functions).
	 */
	void updateInternal(SV valueToStore) throws Exception;
}
