package org.apache.flink.api.common.state;

import org.apache.flink.annotation.PublicEvolving;

/**
 * {@link State} interface for reducing state. Elements can be added to the state, they will
 * be combined using a reduce function. The current state can be inspected.
 *
 * <p>The state is accessed and modified by user functions, and checkpointed consistently
 * by the system as part of the distributed snapshots.
 *
 * <p>The state is only accessible by functions applied on a {@code KeyedStream}. The key is
 * automatically supplied by the system, so the function always sees the value mapped to the
 * key of the current element. That way, the system can handle stream and state partitioning
 * consistently together.
 *
 * @param <T> Type of the value in the operator state
 */
@PublicEvolving
public interface ReducingState<T> extends MergingState<T, T> {}
