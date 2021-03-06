package org.apache.flink.streaming.api.operators;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MetricOptions;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.streamstatus.StreamStatusMaintainer;
import org.apache.flink.streaming.runtime.tasks.OperatorChain;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeCallback;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;

import java.util.concurrent.ScheduledFuture;

/**
 * {@link StreamOperator} for streaming sources.
 *
 * @param <OUT> Type of the output elements
 * @param <SRC> Type of the source function of this stream source operator
 */
@Internal
public class StreamSource<OUT, SRC extends SourceFunction<OUT>> extends AbstractUdfStreamOperator<OUT, SRC> {

	private transient SourceFunction.SourceContext<OUT> ctx;

	private transient volatile boolean canceledOrStopped = false;

	private transient volatile boolean hasSentMaxWatermark = false;

	public StreamSource(SRC sourceFunction) {
		super(sourceFunction);

		this.chainingStrategy = ChainingStrategy.HEAD;
	}

	public void run(final Object lockingObject,
			final StreamStatusMaintainer streamStatusMaintainer,
			final OperatorChain<?, ?> operatorChain) throws Exception {

		run(lockingObject, streamStatusMaintainer, output, operatorChain);
	}

	public void run(final Object lockingObject,
			final StreamStatusMaintainer streamStatusMaintainer,
			final Output<StreamRecord<OUT>> collector,
			final OperatorChain<?, ?> operatorChain) throws Exception {

		final TimeCharacteristic timeCharacteristic = getOperatorConfig().getTimeCharacteristic();

		final Configuration configuration = this.getContainingTask().getEnvironment().getTaskManagerInfo().getConfiguration();
		final long latencyTrackingInterval = getExecutionConfig().isLatencyTrackingConfigured()
			? getExecutionConfig().getLatencyTrackingInterval()
			: configuration.getLong(MetricOptions.LATENCY_INTERVAL);

		LatencyMarksEmitter<OUT> latencyEmitter = null;
		if (latencyTrackingInterval > 0) {
			latencyEmitter = new LatencyMarksEmitter<>(
				getProcessingTimeService(),
				collector,
				latencyTrackingInterval,
				this.getOperatorID(),
				getRuntimeContext().getIndexOfThisSubtask());
		}

		final long watermarkInterval = getRuntimeContext().getExecutionConfig().getAutoWatermarkInterval();

		this.ctx = StreamSourceContexts.getSourceContext(
			timeCharacteristic,
			getProcessingTimeService(),
			lockingObject,
			streamStatusMaintainer,
			collector,
			watermarkInterval,
			-1);

		try {
			userFunction.run(ctx);

			// if we get here, then the user function either exited after being done (finite source)
			// or the function was canceled or stopped. For the finite source case, we should emit
			// a final watermark that indicates that we reached the end of event-time, and end inputs
			// of the operator chain
			if (!isCanceledOrStopped()) {
				// in theory, the subclasses of StreamSource may implement the BoundedOneInput interface,
				// so we still need the following call to end the input
				synchronized (lockingObject) {
					operatorChain.endHeadOperatorInput(1);
				}
			}
		} finally {
			if (latencyEmitter != null) {
				latencyEmitter.close();
			}
		}
	}

	public void advanceToEndOfEventTime() {
		if (!hasSentMaxWatermark) {
			ctx.emitWatermark(Watermark.MAX_WATERMARK);
			hasSentMaxWatermark = true;
		}
	}

	@Override
	public void close() throws Exception {
		try {
			super.close();
			if (!isCanceledOrStopped() && ctx != null) {
				advanceToEndOfEventTime();
			}
		} finally {
				ctx.close();
		}
	}

	public void cancel() {
		// important: marking the source as stopped has to happen before the function is stopped.
		// the flag that tracks this status is volatile, so the memory model also guarantees
		// the happens-before relationship
		markCanceledOrStopped();
		userFunction.cancel();

		ctx.close();
	}

	/**
	 * Marks this source as canceled or stopped.
	 *
	 * <p>This indicates that any exit of the {@link #run(Object, StreamStatusMaintainer, Output)} method
	 * cannot be interpreted as the result of a finite source.
	 */
	protected void markCanceledOrStopped() {
		this.canceledOrStopped = true;
	}

	/**
	 * Checks whether the source has been canceled or stopped.
	 * @return True, if the source is canceled or stopped, false is not.
	 */
	protected boolean isCanceledOrStopped() {
		return canceledOrStopped;
	}

	private static class LatencyMarksEmitter<OUT> {
		private final ScheduledFuture<?> latencyMarkTimer;

		public LatencyMarksEmitter(
				final ProcessingTimeService processingTimeService,
				final Output<StreamRecord<OUT>> output,
				long latencyTrackingInterval,
				final OperatorID operatorId,
				final int subtaskIndex) {

			latencyMarkTimer = processingTimeService.scheduleAtFixedRate(
				new ProcessingTimeCallback() {
					@Override
					public void onProcessingTime(long timestamp) throws Exception {
						try {
							// ProcessingTimeService callbacks are executed under the checkpointing lock
							output.emitLatencyMarker(new LatencyMarker(processingTimeService.getCurrentProcessingTime(), operatorId, subtaskIndex));
						} catch (Throwable t) {
							// we catch the Throwables here so that we don't trigger the processing
							// timer services async exception handler
							LOG.warn("Error while emitting latency marker.", t);
						}
					}
				},
				0L,
				latencyTrackingInterval);
		}

		public void close() {
			latencyMarkTimer.cancel(true);
		}
	}
}
