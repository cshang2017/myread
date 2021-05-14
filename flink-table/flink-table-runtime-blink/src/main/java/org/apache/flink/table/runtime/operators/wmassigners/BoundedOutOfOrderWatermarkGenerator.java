package org.apache.flink.table.runtime.operators.wmassigners;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.generated.WatermarkGenerator;

import javax.annotation.Nullable;

/**
 * A watermark generator for rowtime attributes which are out-of-order by a bounded time interval.
 *
 * <p>Emits watermarks which are the observed timestamp minus the specified delay.
 */
public class BoundedOutOfOrderWatermarkGenerator extends WatermarkGenerator {

	private final long delay;
	private final int rowtimeIndex;

	/**
	 * @param rowtimeIndex the field index of rowtime attribute, the value of rowtime should never be null.
	 * @param delay The delay by which watermarks are behind the observed timestamp.
	 */
	public BoundedOutOfOrderWatermarkGenerator(int rowtimeIndex, long delay) {
		this.delay = delay;
		this.rowtimeIndex = rowtimeIndex;
	}

	@Nullable
	@Override
	public Long currentWatermark(RowData row) {
		return row.getLong(rowtimeIndex) - delay;
	}
}
