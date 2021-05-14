package org.apache.flink.streaming.runtime.io;

import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.checkpoint.channel.ChannelStateWriter;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;

/**
 * Basic interface for inputs of stream operators.
 */
@Internal
public interface StreamTaskInput<T> extends PushingAsyncDataInput<T>, Closeable {
	int UNSPECIFIED = -1;

	/**
	 * Returns the input index of this input.
	 */
	int getInputIndex();

	/**
	 * Prepares to spill the in-flight input buffers as checkpoint snapshot.
	 */
	CompletableFuture<Void> prepareSnapshot(ChannelStateWriter channelStateWriter, long checkpointId) throws IOException;
}
