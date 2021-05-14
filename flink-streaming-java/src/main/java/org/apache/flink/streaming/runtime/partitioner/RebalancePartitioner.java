

package org.apache.flink.streaming.runtime.partitioner;

import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.plugable.SerializationDelegate;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.util.concurrent.ThreadLocalRandom;

/**
 * Partitioner that distributes the data equally by cycling through the output
 * channels.
 *
 * @param <T> Type of the elements in the Stream being rebalanced
 */
@Internal
public class RebalancePartitioner<T> extends StreamPartitioner<T> {
	private static final long serialVersionUID = 1L;

	private int nextChannelToSendTo;

	@Override
	public void setup(int numberOfChannels) {
		super.setup(numberOfChannels);

		nextChannelToSendTo = ThreadLocalRandom.current().nextInt(numberOfChannels);
	}

	@Override
	public int selectChannel(SerializationDelegate<StreamRecord<T>> record) {
		nextChannelToSendTo = (nextChannelToSendTo + 1) % numberOfChannels;
		return nextChannelToSendTo;
	}

	public StreamPartitioner<T> copy() {
		return this;
	}

	@Override
	public String toString() {
		return "REBALANCE";
	}
}
