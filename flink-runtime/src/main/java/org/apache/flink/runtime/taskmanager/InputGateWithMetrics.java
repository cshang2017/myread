package org.apache.flink.runtime.taskmanager;

import org.apache.flink.metrics.Counter;
import org.apache.flink.runtime.checkpoint.channel.ChannelStateReader;
import org.apache.flink.runtime.event.TaskEvent;
import org.apache.flink.runtime.io.network.buffer.BufferReceivedListener;
import org.apache.flink.runtime.io.network.partition.consumer.BufferOrEvent;
import org.apache.flink.runtime.io.network.partition.consumer.IndexedInputGate;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.InputGate;
import org.apache.flink.runtime.metrics.groups.TaskIOMetricGroup;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * This class wraps {@link InputGate} provided by shuffle service and it is mainly
 * used for increasing general input metrics from {@link TaskIOMetricGroup}.
 */
public class InputGateWithMetrics extends IndexedInputGate {

	private final IndexedInputGate inputGate;

	private final Counter numBytesIn;

	public InputGateWithMetrics(IndexedInputGate inputGate, Counter numBytesIn) {
		this.inputGate = checkNotNull(inputGate);
		this.numBytesIn = checkNotNull(numBytesIn);
	}

	@Override
	public CompletableFuture<?> getAvailableFuture() {
		return inputGate.getAvailableFuture();
	}

	@Override
	public void resumeConsumption(int channelIndex) throws IOException {
		inputGate.resumeConsumption(channelIndex);
	}

	@Override
	public int getNumberOfInputChannels() {
		return inputGate.getNumberOfInputChannels();
	}

	@Override
	public InputChannel getChannel(int channelIndex) {
		return inputGate.getChannel(channelIndex);
	}

	@Override
	public int getGateIndex() {
		return inputGate.getGateIndex();
	}

	@Override
	public boolean isFinished() {
		return inputGate.isFinished();
	}

	@Override
	public void setup() throws IOException {
		inputGate.setup();
	}

	@Override
	public CompletableFuture<?> readRecoveredState(ExecutorService executor, ChannelStateReader reader) throws IOException {
		return inputGate.readRecoveredState(executor, reader);
	}

	@Override
	public void requestPartitions() throws IOException {
		inputGate.requestPartitions();
	}

	@Override
	public Optional<BufferOrEvent> getNext() throws IOException, InterruptedException {
		return inputGate.getNext().map(this::updateMetrics);
	}

	@Override
	public Optional<BufferOrEvent> pollNext() throws IOException, InterruptedException {
		return inputGate.pollNext().map(this::updateMetrics);
	}

	@Override
	public void sendTaskEvent(TaskEvent event) throws IOException {
		inputGate.sendTaskEvent(event);
	}

	@Override
	public void close() throws Exception {
		inputGate.close();
	}

	@Override
	public void registerBufferReceivedListener(BufferReceivedListener listener) {
		inputGate.registerBufferReceivedListener(listener);
	}

	private BufferOrEvent updateMetrics(BufferOrEvent bufferOrEvent) {
		numBytesIn.inc(bufferOrEvent.getSize());
		return bufferOrEvent;
	}
}
