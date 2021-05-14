package org.apache.flink.streaming.api.operators.source;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.eventtime.TimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.streaming.runtime.io.PushingAsyncDataInput;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;

import java.time.Duration;

/**
 * Basic interface for the timestamp extraction and watermark generation logic for the
 * {@link org.apache.flink.api.connector.source.SourceReader}.
 *
 * <p>Implementations of this class may or may not actually perform certain tasks, like watermark
 * generation. For example, the batch-oriented implementation typically skips all watermark generation
 * logic.
 *
 * @param <T> The type of the emitted records.
 */
@Internal
public interface TimestampsAndWatermarks<T> {

	/**
	 * Creates the ReaderOutput for the source reader, than internally runs the timestamp extraction and
	 * watermark generation.
	 */
	ReaderOutput<T> createMainOutput(PushingAsyncDataInput.DataOutput<T> output);

	/**
	 * Starts emitting periodic watermarks, if this implementation produces watermarks, and if
	 * periodic watermarks are configured.
	 *
	 * <p>Periodic watermarks are produced by periodically calling the
	 * {@link org.apache.flink.api.common.eventtime.WatermarkGenerator#onPeriodicEmit(WatermarkOutput)} method
	 * of the underlying Watermark Generators.
	 */
	void startPeriodicWatermarkEmits();

	/**
	 * Stops emitting periodic watermarks.
	 */
	void stopPeriodicWatermarkEmits();

	// ------------------------------------------------------------------------
	//  factories
	// ------------------------------------------------------------------------

	static <E> TimestampsAndWatermarks<E> createStreamingEventTimeLogic(
			WatermarkStrategy<E> watermarkStrategy,
			MetricGroup metrics,
			ProcessingTimeService timeService,
			long periodicWatermarkIntervalMillis) {

		final TimestampsAndWatermarksContext context = new TimestampsAndWatermarksContext(metrics);
		final TimestampAssigner<E> timestampAssigner = watermarkStrategy.createTimestampAssigner(context);

		return new StreamingTimestampsAndWatermarks<>(
			timestampAssigner, watermarkStrategy, context, timeService, Duration.ofMillis(periodicWatermarkIntervalMillis));
	}

	static <E> TimestampsAndWatermarks<E> createBatchEventTimeLogic(
			WatermarkStrategy<E> watermarkStrategy,
			MetricGroup metrics) {

		final TimestampsAndWatermarksContext context = new TimestampsAndWatermarksContext(metrics);
		final TimestampAssigner<E> timestampAssigner = watermarkStrategy.createTimestampAssigner(context);

		return new BatchTimestampsAndWatermarks<>(timestampAssigner);
	}
}
