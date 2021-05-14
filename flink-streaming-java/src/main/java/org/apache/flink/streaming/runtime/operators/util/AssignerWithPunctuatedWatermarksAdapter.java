package org.apache.flink.streaming.runtime.operators.util;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.eventtime.TimestampAssigner;
import org.apache.flink.api.common.eventtime.TimestampAssignerSupplier;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * An adapter that wraps a {@link AssignerWithPunctuatedWatermarks} into a
 * {@link WatermarkGenerator}.
 */
@Internal
@SuppressWarnings("deprecation")
public final class AssignerWithPunctuatedWatermarksAdapter<T> implements WatermarkGenerator<T> {

	private final AssignerWithPunctuatedWatermarks<T> wms;

	public AssignerWithPunctuatedWatermarksAdapter(AssignerWithPunctuatedWatermarks<T> wms) {
		this.wms = checkNotNull(wms);
	}

	@Override
	public void onEvent(T event, long eventTimestamp, WatermarkOutput output) {
		final org.apache.flink.streaming.api.watermark.Watermark next =
			wms.checkAndGetNextWatermark(event, eventTimestamp);

		if (next != null) {
			output.emitWatermark(new Watermark(next.getTimestamp()));
		}
	}

	@Override
	public void onPeriodicEmit(WatermarkOutput output) {}

	// ------------------------------------------------------------------------

	/**
	 * A WatermarkStrategy that returns an {@link AssignerWithPunctuatedWatermarks} wrapped as a
	 * {@link WatermarkGenerator}.
	 */
	public static final class Strategy<T> implements WatermarkStrategy<T> {
		private static final long serialVersionUID = 1L;

		private final AssignerWithPunctuatedWatermarks<T> wms;

		public Strategy(AssignerWithPunctuatedWatermarks<T> wms) {
			this.wms = checkNotNull(wms);
		}

		@Override
		public TimestampAssigner<T> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
			return wms;
		}

		@Override
		public WatermarkGenerator<T> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
			return new AssignerWithPunctuatedWatermarksAdapter<>(wms);
		}
	}
}
