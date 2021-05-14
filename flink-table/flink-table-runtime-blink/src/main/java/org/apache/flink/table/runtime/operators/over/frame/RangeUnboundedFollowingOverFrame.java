
package org.apache.flink.table.runtime.operators.over.frame;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.runtime.context.ExecutionContext;
import org.apache.flink.table.runtime.generated.GeneratedAggsHandleFunction;
import org.apache.flink.table.runtime.generated.GeneratedRecordComparator;
import org.apache.flink.table.runtime.generated.RecordComparator;
import org.apache.flink.table.runtime.util.ResettableExternalBuffer;
import org.apache.flink.table.types.logical.RowType;

/**
 * The range unboundedFollowing window frame calculates frames with the following SQL form:
 * ... RANGE BETWEEN [window frame preceding] AND UNBOUNDED FOLLOWING
 * [window frame preceding] ::= [unsigned_value_specification] PRECEDING | CURRENT ROW
 *
 * <p>e.g.: ... RANGE BETWEEN 1 PRECEDING AND UNBOUNDED FOLLOWING.
 */
public class RangeUnboundedFollowingOverFrame extends UnboundedFollowingOverFrame {

	private GeneratedRecordComparator boundComparator;
	private RecordComparator lbound;

	public RangeUnboundedFollowingOverFrame(
			RowType valueType,
			GeneratedAggsHandleFunction aggsHandleFunction,
			GeneratedRecordComparator boundComparator) {
		super(valueType, aggsHandleFunction);
		this.boundComparator = boundComparator;
	}

	@Override
	public void open(ExecutionContext ctx) throws Exception {
		super.open(ctx);
		lbound = boundComparator.newInstance(ctx.getRuntimeContext().getUserCodeClassLoader());
		this.boundComparator = null;
	}

	@Override
	public RowData process(int index, RowData current) throws Exception {
		boolean bufferUpdated = index == 0;

		// Ignore all the rows from the buffer for which the input row value is smaller than
		// the output row lower bound.
		ResettableExternalBuffer.BufferIterator iterator = input.newIterator(inputIndex);

		BinaryRowData nextRow = OverWindowFrame.getNextOrNull(iterator);
		while (nextRow != null && lbound.compare(nextRow, current) < 0) {
			inputIndex += 1;
			bufferUpdated = true;
			nextRow = OverWindowFrame.getNextOrNull(iterator);
		}

		return accumulateIterator(bufferUpdated, nextRow, iterator);
	}
}
