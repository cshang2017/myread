package org.apache.flink.streaming.runtime.metrics;

import org.apache.flink.metrics.Gauge;

import java.util.Arrays;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * A {@link Gauge} for exposing the minimum watermark of chosen {@link WatermarkGauge}s.
 */
public class MinWatermarkGauge implements Gauge<Long> {
	private final WatermarkGauge[] watermarkGauges;

	public MinWatermarkGauge(WatermarkGauge ...watermarkGauges) {
		checkArgument(watermarkGauges.length > 0);
		this.watermarkGauges = watermarkGauges;
	}

	@Override
	public Long getValue() {
		return Arrays.stream(watermarkGauges)
			.mapToLong(WatermarkGauge::getValue)
			.min()
			.orElse(0);
	}
}
