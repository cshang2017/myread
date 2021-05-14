package org.apache.flink.streaming.connectors.kinesis;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.streaming.connectors.kinesis.model.StreamShardHandle;

import java.io.Serializable;

/**
 * Utility to map Kinesis shards to Flink subtask indices. Users can implement this interface to optimize
 * distribution of shards over subtasks. See {@link #assign(StreamShardHandle, int)}  for details.
 */
@PublicEvolving
public interface KinesisShardAssigner extends Serializable {

    /**
	 * Returns the index of the target subtask that a specific shard should be
	 * assigned to. For return values outside the subtask range, modulus operation will
	 * be applied automatically, hence it is also valid to just return a hash code.
	 *
	 * <p>The resulting distribution of shards should have the following contract:
	 * <ul>
	 *     <li>1. Uniform distribution across subtasks</li>
	 *     <li>2. Deterministic, calls for a given shard always return same index.</li>
	 * </ul>
	 *
	 * <p>The above contract is crucial and cannot be broken. Consumer subtasks rely on this
	 * contract to filter out shards that they should not subscribe to, guaranteeing
	 * that each shard of a stream will always be assigned to one subtask in a
	 * uniformly distributed manner.
	 *
     * @param shard the shard to determine
     * @param numParallelSubtasks total number of subtasks
     * @return target index, if index falls outside of the range, modulus operation will be applied
     */
	int assign(StreamShardHandle shard, int numParallelSubtasks);
}
