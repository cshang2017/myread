package org.apache.flink.streaming.connectors.kinesis;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kinesis.internals.DynamoDBStreamsDataFetcher;
import org.apache.flink.streaming.connectors.kinesis.internals.KinesisDataFetcher;
import org.apache.flink.streaming.connectors.kinesis.serialization.KinesisDeserializationSchema;

import java.util.List;
import java.util.Properties;

/**
 * Consume events from DynamoDB streams.
 *
 * @param <T> the type of data emitted
 */
public class FlinkDynamoDBStreamsConsumer<T> extends FlinkKinesisConsumer<T> {

	/**
	 * Constructor of FlinkDynamoDBStreamsConsumer.
	 *
	 * @param stream stream to consume
	 * @param deserializer deserialization schema
	 * @param config config properties
	 */
	public FlinkDynamoDBStreamsConsumer(
			String stream,
			DeserializationSchema<T> deserializer,
			Properties config) {
		super(stream, deserializer, config);
	}

	/**
	 * Constructor of FlinkDynamodbStreamConsumer.
	 *
	 * @param streams list of streams to consume
	 * @param deserializer  deserialization schema
	 * @param config config properties
	 */
	public FlinkDynamoDBStreamsConsumer(
			List<String> streams,
			KinesisDeserializationSchema deserializer,
			Properties config) {
		super(streams, deserializer, config);
	}

	@Override
	protected KinesisDataFetcher<T> createFetcher(
			List<String> streams,
			SourceFunction.SourceContext<T> sourceContext,
			RuntimeContext runtimeContext,
			Properties configProps,
			KinesisDeserializationSchema<T> deserializationSchema) {
		return new DynamoDBStreamsDataFetcher<T>(
				streams,
				sourceContext,
				runtimeContext,
				configProps,
				deserializationSchema,
				getShardAssigner());
	}
}
