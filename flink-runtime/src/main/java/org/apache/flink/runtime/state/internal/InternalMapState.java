package org.apache.flink.runtime.state.internal;

import org.apache.flink.api.common.state.MapState;

import java.util.Map;

/**
 * The peer to the {@link MapState} in the internal state type hierarchy.
 *
 * <p>See {@link InternalKvState} for a description of the internal state hierarchy.
 *
 * @param <K> The type of key the state is associated to
 * @param <N> The type of the namespace
 * @param <UK> Type of the values folded into the state
 * @param <UV> Type of the value in the state
 */
public interface InternalMapState<K, N, UK, UV> extends InternalKvState<K, N, Map<UK, UV>>, MapState<UK, UV> {}
