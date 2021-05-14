package org.apache.flink.table.runtime.operators.over.frame;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.context.ExecutionContext;
import org.apache.flink.table.runtime.generated.GeneratedAggsHandleFunction;
import org.apache.flink.table.runtime.generated.GeneratedRecordComparator;
import org.apache.flink.table.runtime.generated.RecordComparator;

/**
 * The range UnboundPreceding window frame calculates frames with the following SQL form:
 * ... RANGE BETWEEN UNBOUNDED PRECEDING AND [window frame following]
 * [window frame following] ::= [unsigned_value_specification] FOLLOWING | CURRENT ROW
 *
 * <p>e.g.: ... RANGE BETWEEN UNBOUNDED PRECEDING AND 1 FOLLOWING.
 */
public class RangeUnboundedPrecedingOverFrame extends UnboundedPrecedingOverFrame {

	private GeneratedRecordComparator boundComparator;
	private RecordComparator rbound;

	public RangeUnboundedPrecedingOverFrame(
			GeneratedAggsHandleFunction aggsHandleFunction,
			GeneratedRecordComparator boundComparator) {
		super(aggsHandleFunction);
		this.boundComparator = boundComparator;
	}

	@Override
	public void open(ExecutionContext ctx) throws Exception {
		super.open(ctx);
		rbound = boundComparator.newInstance(ctx.getRuntimeContext().getUserCodeClassLoader());
		this.boundComparator = null;
	}

	@Override
	public RowData process(int index, RowData current) throws Exception {
		boolean bufferUpdated = index == 0;

		// Add all rows to the aggregates for which the input row value is equal to or less than
		// the output row upper bound.
		while (nextRow != null && rbound.compare(nextRow, current) <= 0) {
			processor.accumulate(nextRow);
			nextRow = OverWindowFrame.getNextOrNull(inputIterator);
			bufferUpdated = true;
		}
		if (bufferUpdated) {
			accValue = processor.getValue();
		}
		return accValue;
	}
}
