package org.apache.flink.api.common.eventtime;

import org.apache.flink.annotation.Public;
import org.apache.flink.api.common.ExecutionConfig;

/**
 * The {@code WatermarkGenerator} generates watermarks either based on events or
 * periodically (in a fixed interval).
 *
 * <p><b>Note:</b> This WatermarkGenerator subsumes the previous distinction between the
 * {@code AssignerWithPunctuatedWatermarks} and the {@code AssignerWithPeriodicWatermarks}.
 */
@Public
public interface WatermarkGenerator<T> {

	/**
	 * Called for every event, allows the watermark generator to examine and remember the
	 * event timestamps, or to emit a watermark based on the event itself.
	 */
	void onEvent(T event, long eventTimestamp, WatermarkOutput output);

	/**
	 * Called periodically, and might emit a new watermark, or not.
	 *
	 * <p>The interval in which this method is called and Watermarks are generated
	 * depends on {@link ExecutionConfig#getAutoWatermarkInterval()}.
	 */
	void onPeriodicEmit(WatermarkOutput output);
}
