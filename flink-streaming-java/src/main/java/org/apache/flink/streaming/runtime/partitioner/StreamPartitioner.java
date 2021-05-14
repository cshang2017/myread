package org.apache.flink.streaming.runtime.partitioner;

import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.io.network.api.writer.ChannelSelector;
import org.apache.flink.runtime.plugable.SerializationDelegate;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.io.Serializable;

/**
 * A special {@link ChannelSelector} for use in streaming programs.
 */
@Internal
public abstract class StreamPartitioner<T> implements
		ChannelSelector<SerializationDelegate<StreamRecord<T>>>, Serializable {

	protected int numberOfChannels;

	@Override
	public void setup(int numberOfChannels) {
		this.numberOfChannels = numberOfChannels;
	}

	@Override
	public boolean isBroadcast() {
		return false;
	}

	public abstract StreamPartitioner<T> copy();
}
