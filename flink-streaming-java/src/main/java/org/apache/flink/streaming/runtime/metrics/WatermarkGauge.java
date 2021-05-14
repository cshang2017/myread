package org.apache.flink.streaming.runtime.metrics;

import org.apache.flink.metrics.Gauge;

/**
 * A {@link Gauge} for exposing the current input/output watermark.
 */
public class WatermarkGauge implements Gauge<Long> {

	private volatile long currentWatermark = Long.MIN_VALUE;

	public void setCurrentWatermark(long watermark) {
		currentWatermark = watermark;
	}

	@Override
	public Long getValue() {
		return currentWatermark;
	}
}
