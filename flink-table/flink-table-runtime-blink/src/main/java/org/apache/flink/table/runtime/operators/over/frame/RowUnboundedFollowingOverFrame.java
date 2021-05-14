package org.apache.flink.table.runtime.operators.over.frame;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.runtime.generated.GeneratedAggsHandleFunction;
import org.apache.flink.table.runtime.util.ResettableExternalBuffer;
import org.apache.flink.table.types.logical.RowType;

/**
 * The row unboundedFollowing window frame calculates frames with the following SQL form:
 * ... ROW BETWEEN [window frame preceding] AND UNBOUNDED FOLLOWING
 * [window frame preceding] ::= [unsigned_value_specification] PRECEDING | CURRENT ROW
 *
 * <p>e.g.: ... ROW BETWEEN 1 PRECEDING AND UNBOUNDED FOLLOWING.
 */
public class RowUnboundedFollowingOverFrame extends UnboundedFollowingOverFrame {

	private long leftBound;

	public RowUnboundedFollowingOverFrame(
			RowType valueType,
			GeneratedAggsHandleFunction aggsHandleFunction,
			long leftBound) {
		super(valueType, aggsHandleFunction);
		this.leftBound = leftBound;
	}

	@Override
	public RowData process(int index, RowData current) throws Exception {
		boolean bufferUpdated = index == 0;

		// Ignore all the rows from the buffer util left bound.
		ResettableExternalBuffer.BufferIterator iterator = input.newIterator(inputIndex);

		BinaryRowData nextRow = OverWindowFrame.getNextOrNull(iterator);
		while (nextRow != null && inputIndex < index + leftBound) {
			inputIndex += 1;
			bufferUpdated = true;
			nextRow = OverWindowFrame.getNextOrNull(iterator);
		}

		return accumulateIterator(bufferUpdated, nextRow, iterator);
	}
}
