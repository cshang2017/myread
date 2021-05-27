package org.apache.flink.runtime.state.internal;

import org.apache.flink.api.common.state.ReducingState;

/**
 * The peer to the {@link ReducingState} in the internal state type hierarchy.
 * 
 * <p>See {@link InternalKvState} for a description of the internal state hierarchy.
 *
 * @param <K> The type of key the state is associated to
 * @param <N> The type of the namespace
 * @param <T> The type of elements in the aggregated by the ReduceFunction
 */
public interface InternalReducingState<K, N, T> extends InternalMergingState<K, N, T, T, T>, ReducingState<T> {}
