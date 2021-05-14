
package org.apache.flink.table.runtime.operators.over.frame;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.generated.GeneratedAggsHandleFunction;
import org.apache.flink.table.runtime.util.ResettableExternalBuffer;

/**
 * The row UnboundPreceding window frame calculates frames with the following SQL form:
 * ... ROW BETWEEN UNBOUNDED PRECEDING AND [window frame following]
 * [window frame following] ::= [unsigned_value_specification] FOLLOWING | CURRENT ROW
 *
 * <p>e.g.: ... ROW BETWEEN UNBOUNDED PRECEDING AND 1 FOLLOWING.
 */
public class RowUnboundedPrecedingOverFrame extends UnboundedPrecedingOverFrame {

	private long rightBound;

	/**
	 * Index of the right bound input row.
	 */
	private long inputRightIndex = 0;

	public RowUnboundedPrecedingOverFrame(
			GeneratedAggsHandleFunction aggsHandleFunction, long rightBound) {
		super(aggsHandleFunction);
		this.rightBound = rightBound;
	}

	@Override
	public void prepare(ResettableExternalBuffer rows) throws Exception {
		super.prepare(rows);
		inputRightIndex = 0;
	}

	@Override
	public RowData process(int index, RowData current) throws Exception {
		boolean bufferUpdated = index == 0;

		// Add all rows to the aggregates util right bound.
		while (nextRow != null && inputRightIndex <= index + rightBound) {
			processor.accumulate(nextRow);
			nextRow = OverWindowFrame.getNextOrNull(inputIterator);
			inputRightIndex += 1;
			bufferUpdated = true;
		}
		if (bufferUpdated) {
			accValue = processor.getValue();
		}
		return accValue;
	}
}
