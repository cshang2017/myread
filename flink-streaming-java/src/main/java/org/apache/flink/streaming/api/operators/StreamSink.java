package org.apache.flink.streaming.api.operators;

import org.apache.flink.annotation.Internal;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;

/**
 * A {@link StreamOperator} for executing {@link SinkFunction SinkFunctions}.
 */
@Internal
public class StreamSink<IN> extends AbstractUdfStreamOperator<Object, SinkFunction<IN>>
		implements OneInputStreamOperator<IN, Object> {


	private transient SimpleContext sinkContext;

	/** We listen to this ourselves because we don't have an {@link InternalTimerService}. */
	private long currentWatermark = Long.MIN_VALUE;

	public StreamSink(SinkFunction<IN> sinkFunction) {
		super(sinkFunction);
		chainingStrategy = ChainingStrategy.ALWAYS;
	}

	@Override
	public void open() throws Exception {
		super.open();

		this.sinkContext = new SimpleContext<>(getProcessingTimeService());
	}

	@Override
	public void processElement(StreamRecord<IN> element) throws Exception {
		sinkContext.element = element;
		userFunction.invoke(element.getValue(), sinkContext);
	}

	@Override
	protected void reportOrForwardLatencyMarker(LatencyMarker marker) {
		// all operators are tracking latencies
		this.latencyStats.reportLatency(marker);

		// sinks don't forward latency markers
	}

	@Override
	public void processWatermark(Watermark mark) throws Exception {
		super.processWatermark(mark);
		this.currentWatermark = mark.getTimestamp();
	}

	private class SimpleContext<IN> implements SinkFunction.Context<IN> {

		private StreamRecord<IN> element;

		private final ProcessingTimeService processingTimeService;

		public SimpleContext(ProcessingTimeService processingTimeService) {
			this.processingTimeService = processingTimeService;
		}

		@Override
		public long currentProcessingTime() {
			return processingTimeService.getCurrentProcessingTime();
		}

		@Override
		public long currentWatermark() {
			return currentWatermark;
		}

		@Override
		public Long timestamp() {
			if (element.hasTimestamp()) {
				return element.getTimestamp();
			}
			return null;
		}
	}
}
