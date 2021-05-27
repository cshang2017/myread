

package org.apache.flink.runtime.state.internal;

import org.apache.flink.api.common.state.AggregatingState;

/**
 * The peer to the {@link AggregatingState} in the internal state type hierarchy.
 * 
 * <p>See {@link InternalKvState} for a description of the internal state hierarchy.
 *
 * @param <K> The type of key the state is associated to
 * @param <N> The type of the namespace
 * @param <IN> Type of the value added to the state
 * @param <SV> The type of elements in the state
 * @param <OUT> Type of the value extracted from the state
 */
public interface InternalAggregatingState<K, N, IN, SV, OUT> extends InternalMergingState<K, N, IN, SV, OUT>, AggregatingState<IN, OUT> {}
