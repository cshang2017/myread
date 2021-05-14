package org.apache.flink.streaming.api.operators.collect;

import org.apache.flink.annotation.Internal;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.transformations.SinkTransformation;

/**
 * A {@link DataStreamSink} which is used to collect results of a data stream.
 * It completely overwrites {@link DataStreamSink} so that its own transformation is manipulated.
 */
@Internal
public class CollectStreamSink<T> extends DataStreamSink<T> {

	private final SinkTransformation<T> transformation;

	public CollectStreamSink(DataStream<T> inputStream, CollectSinkOperatorFactory<T> factory) {
		super(inputStream, (CollectSinkOperator<T>) factory.getOperator());
		this.transformation = new SinkTransformation<>(
			inputStream.getTransformation(),
			"Collect Stream Sink",
			factory,
			1);
	}

	@Override
	public SinkTransformation<T> getTransformation() {
		return transformation;
	}

	@Override
	public DataStreamSink<T> name(String name) {
		transformation.setName(name);
		return this;
	}

	@Override
	public DataStreamSink<T> uid(String uid) {
		transformation.setUid(uid);
		return this;
	}

	@Override
	public DataStreamSink<T> setUidHash(String uidHash) {
		transformation.setUidHash(uidHash);
		return this;
	}

	@Override
	public DataStreamSink<T> setParallelism(int parallelism) {
		transformation.setParallelism(parallelism);
		return this;
	}

	@Override
	public DataStreamSink<T> disableChaining() {
		this.transformation.setChainingStrategy(ChainingStrategy.NEVER);
		return this;
	}

	@Override
	public DataStreamSink<T> slotSharingGroup(String slotSharingGroup) {
		transformation.setSlotSharingGroup(slotSharingGroup);
		return this;
	}
}
