package org.apache.flink.streaming.runtime.io;

import org.apache.flink.annotation.Internal;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.runtime.checkpoint.channel.ChannelStateWriter;
import org.apache.flink.runtime.io.AvailabilityProvider;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;

/**
 * Interface for processing records by {@link org.apache.flink.streaming.runtime.tasks.StreamTask}.
 */
@Internal
public interface StreamInputProcessor extends AvailabilityProvider, Closeable {
	/**
	 * @return input status to estimate whether more records can be processed immediately or not.
	 * If there are no more records available at the moment and the caller should check finished
	 * state and/or {@link #getAvailableFuture()}.
	 */
	InputStatus processInput() throws Exception;

	CompletableFuture<Void> prepareSnapshot(ChannelStateWriter channelStateWriter, long checkpointId) throws IOException;
}
