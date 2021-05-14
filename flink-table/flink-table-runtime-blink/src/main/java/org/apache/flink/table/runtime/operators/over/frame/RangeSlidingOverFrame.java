package org.apache.flink.table.runtime.operators.over.frame;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.context.ExecutionContext;
import org.apache.flink.table.runtime.generated.GeneratedAggsHandleFunction;
import org.apache.flink.table.runtime.generated.GeneratedRecordComparator;
import org.apache.flink.table.runtime.generated.RecordComparator;
import org.apache.flink.table.types.logical.RowType;

/**
 * The range sliding window frame calculates frames with the following SQL form:
 * ... RANGE BETWEEN [window frame preceding] AND [window frame following]
 * [window frame preceding] ::= [unsigned_value_specification] PRECEDING | CURRENT ROW
 * [window frame following] ::= [unsigned_value_specification] FOLLOWING | CURRENT ROW
 *
 * <p>e.g.: ... RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING.
 */
public class RangeSlidingOverFrame extends SlidingOverFrame {

	private GeneratedRecordComparator lboundComparator;
	private GeneratedRecordComparator rboundComparator;

	private RecordComparator lbound;
	private RecordComparator rbound;

	/**
	 * @param lboundComparator comparator used to identify the lower bound of an output row.
	 * @param rboundComparator comparator used to identify the upper bound of an output row.
	 */
	public RangeSlidingOverFrame(
			RowType inputType,
			RowType valueType,
			GeneratedAggsHandleFunction aggsHandleFunction,
			GeneratedRecordComparator lboundComparator,
			GeneratedRecordComparator rboundComparator) {
		super(inputType, valueType, aggsHandleFunction);
		this.lboundComparator = lboundComparator;
		this.rboundComparator = rboundComparator;
	}

	@Override
	public void open(ExecutionContext ctx) throws Exception {
		super.open(ctx);
		ClassLoader cl = ctx.getRuntimeContext().getUserCodeClassLoader();
		lbound = lboundComparator.newInstance(cl);
		rbound = rboundComparator.newInstance(cl);

		this.lboundComparator = null;
		this.rboundComparator = null;
	}

	@Override
	public RowData process(int index, RowData current) throws Exception {
		boolean bufferUpdated = index == 0;

		// Drop all rows from the buffer for which the input row value is smaller than
		// the output row lower bound.
		while (!buffer.isEmpty() && lbound.compare(buffer.peek(), current) < 0) {
			buffer.remove();
			bufferUpdated = true;
		}

		// Add all rows to the buffer for which the input row value is equal to or less than
		// the output row upper bound.
		while (nextRow != null && rbound.compare(nextRow, current) <= 0) {
			if (lbound.compare(nextRow, current) >= 0) {
				buffer.add(inputSer.copy(nextRow));
				bufferUpdated = true;
			}
			nextRow = OverWindowFrame.getNextOrNull(inputIterator);
		}

		return accumulateBuffer(bufferUpdated);
	}
}
