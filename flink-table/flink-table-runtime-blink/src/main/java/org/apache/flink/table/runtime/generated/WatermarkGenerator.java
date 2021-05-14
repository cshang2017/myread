

package org.apache.flink.table.runtime.generated;

import org.apache.flink.api.common.functions.AbstractRichFunction;
import org.apache.flink.table.data.RowData;

import javax.annotation.Nullable;

/**
 * The {@link WatermarkGenerator} is used to generate watermark based the input elements.
 */
public abstract class WatermarkGenerator extends AbstractRichFunction {


	/**
	 * Returns the watermark for the current row or null if no watermark should be generated.
	 *
	 * @param row The current row.
	 * @return The watermark for this row or null if no watermark should be generated.
	 */
	@Nullable
	public abstract Long currentWatermark(RowData row) throws Exception;
}
