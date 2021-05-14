package org.apache.flink.streaming.runtime.partitioner;

/**
 * Interface for {@link StreamPartitioner} which have to be configured with the maximum parallelism
 * of the stream transformation. The configure method is called by the StreamGraph when adding
 * internal edges.
 *
 * <p>This interface is required since the stream partitioners are instantiated eagerly. Due to that
 * the maximum parallelism might not have been determined and needs to be set at a stage when the
 * maximum parallelism could have been determined.
 */
public interface ConfigurableStreamPartitioner {

	/**
	 * Configure the {@link StreamPartitioner} with the maximum parallelism of the down stream
	 * operator.
	 *
	 * @param maxParallelism Maximum parallelism of the down stream operator.
	 */
	void configure(int maxParallelism);
}
