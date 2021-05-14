

package org.apache.flink.api.common.eventtime;

import org.apache.flink.annotation.Public;

/**
 * An implementation of a {@link WatermarkGenerator} that generates no Watermarks.
 */
@Public
public final class NoWatermarksGenerator<E> implements WatermarkGenerator<E> {

	@Override
	public void onEvent(E event, long eventTimestamp, WatermarkOutput output) {}

	@Override
	public void onPeriodicEmit(WatermarkOutput output) {}
}
