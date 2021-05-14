package org.apache.flink.streaming.connectors.kinesis.internals;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kinesis.KinesisShardAssigner;
import org.apache.flink.streaming.connectors.kinesis.metrics.ShardMetricsReporter;
import org.apache.flink.streaming.connectors.kinesis.model.DynamoDBStreamsShardHandle;
import org.apache.flink.streaming.connectors.kinesis.model.SequenceNumber;
import org.apache.flink.streaming.connectors.kinesis.model.StreamShardHandle;
import org.apache.flink.streaming.connectors.kinesis.proxy.DynamoDBStreamsProxy;
import org.apache.flink.streaming.connectors.kinesis.serialization.KinesisDeserializationSchema;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Dynamodb streams data fetcher.
 * @param <T> type of fetched data.
 */
public class DynamoDBStreamsDataFetcher<T> extends KinesisDataFetcher<T> {
	private boolean shardIdFormatCheck = false;

	/**
	 * Constructor.
	 *
	 * @param streams list of streams to fetch data
	 * @param sourceContext source context
	 * @param runtimeContext runtime context
	 * @param configProps config properties
	 * @param deserializationSchema deserialization schema
	 * @param shardAssigner shard assigner
	 */
	public DynamoDBStreamsDataFetcher(List<String> streams,
		SourceFunction.SourceContext<T> sourceContext,
		RuntimeContext runtimeContext,
		Properties configProps,
		KinesisDeserializationSchema<T> deserializationSchema,
		KinesisShardAssigner shardAssigner) {

		super(streams,
			sourceContext,
			sourceContext.getCheckpointLock(),
			runtimeContext,
			configProps,
			deserializationSchema,
			shardAssigner,
			null,
			null,
			new AtomicReference<>(),
			new ArrayList<>(),
			createInitialSubscribedStreamsToLastDiscoveredShardsState(streams),
			// use DynamoDBStreamsProxy
			DynamoDBStreamsProxy::create);
	}

	@Override
	protected boolean shouldAdvanceLastDiscoveredShardId(String shardId, String lastSeenShardIdOfStream) {
		if (DynamoDBStreamsShardHandle.compareShardIds(shardId, lastSeenShardIdOfStream) <= 0) {
			// shardID update is valid only if the given shard id is greater
			// than the previous last seen shard id of the stream.
			return false;
		}

		return true;
	}

	/**
	 * Create a new DynamoDB streams shard consumer.
	 *
	 * @param subscribedShardStateIndex the state index of the shard this consumer is subscribed to
	 * @param handle stream handle
	 * @param lastSeqNum last sequence number
	 * @param shardMetricsReporter the reporter to report metrics to
	 * @return
	 */
	@Override
	protected ShardConsumer<T> createShardConsumer(
		Integer subscribedShardStateIndex,
		StreamShardHandle handle,
		SequenceNumber lastSeqNum,
		ShardMetricsReporter shardMetricsReporter,
		KinesisDeserializationSchema<T> shardDeserializer) {

		return new ShardConsumer(
			this,
			subscribedShardStateIndex,
			handle,
			lastSeqNum,
			DynamoDBStreamsProxy.create(getConsumerConfiguration()),
			shardMetricsReporter,
			shardDeserializer);
	}
}