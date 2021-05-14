package org.apache.flink.streaming.runtime.io;

import org.apache.flink.annotation.Internal;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.runtime.checkpoint.channel.ChannelStateWriter;
import org.apache.flink.streaming.runtime.io.PushingAsyncDataInput.DataOutput;
import org.apache.flink.streaming.runtime.tasks.OperatorChain;


import java.io.IOException;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Input reader for {@link org.apache.flink.streaming.runtime.tasks.OneInputStreamTask}.
 *
 * @param <IN> The type of the record that can be read with this record reader.
 */
@Internal
public final class StreamOneInputProcessor<IN> implements StreamInputProcessor {


	private final StreamTaskInput<IN> input;
	private final DataOutput<IN> output;

	private final OperatorChain<?, ?> operatorChain;

	public StreamOneInputProcessor(
			StreamTaskInput<IN> input,
			DataOutput<IN> output,
			OperatorChain<?, ?> operatorChain) {

		this.input = checkNotNull(input);
		this.output = checkNotNull(output);
		this.operatorChain = checkNotNull(operatorChain);
	}

	@Override
	public CompletableFuture<?> getAvailableFuture() {
		return input.getAvailableFuture();
	}

	@Override
	public InputStatus processInput() throws Exception {
		InputStatus status = input.emitNext(output);

		if (status == InputStatus.END_OF_INPUT) {
			operatorChain.endHeadOperatorInput(1);
		}

		return status;
	}

	@Override
	public CompletableFuture<Void> prepareSnapshot(
			ChannelStateWriter channelStateWriter,
			long checkpointId) throws IOException {
		return input.prepareSnapshot(channelStateWriter, checkpointId);
	}

	@Override
	public void close() throws IOException {
		input.close();
	}
}
