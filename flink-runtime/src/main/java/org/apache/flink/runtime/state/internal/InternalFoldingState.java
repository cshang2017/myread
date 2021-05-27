package org.apache.flink.runtime.state.internal;

import org.apache.flink.api.common.state.FoldingState;

/**
 * The peer to the {@link FoldingState} in the internal state type hierarchy.
 * 
 * <p>See {@link InternalKvState} for a description of the internal state hierarchy.
 *
 * @param <K> The type of key the state is associated to
 * @param <N> The type of the namespace
 * @param <T> Type of the values folded into the state
 * @param <ACC> Type of the value in the state
 *
 * @deprecated will be removed in a future version
 */
@Deprecated
public interface InternalFoldingState<K, N, T, ACC> extends InternalAppendingState<K, N, T, ACC, ACC>, FoldingState<T, ACC> {}
