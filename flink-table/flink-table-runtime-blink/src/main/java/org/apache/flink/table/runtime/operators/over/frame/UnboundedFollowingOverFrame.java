package org.apache.flink.table.runtime.operators.over.frame;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.runtime.context.ExecutionContext;
import org.apache.flink.table.runtime.dataview.PerKeyStateDataViewStore;
import org.apache.flink.table.runtime.generated.AggsHandleFunction;
import org.apache.flink.table.runtime.generated.GeneratedAggsHandleFunction;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.runtime.util.ResettableExternalBuffer;
import org.apache.flink.table.types.logical.RowType;

/**
 * The UnboundedFollowing window frame.
 * See {@link RowUnboundedFollowingOverFrame} and {@link RangeUnboundedFollowingOverFrame}.
 */
public abstract class UnboundedFollowingOverFrame implements OverWindowFrame {

	private GeneratedAggsHandleFunction aggsHandleFunction;
	private final RowType valueType;

	private AggsHandleFunction processor;
	private RowData accValue;

	/** Rows of the partition currently being processed. */
	ResettableExternalBuffer input;

	private RowDataSerializer valueSer;

	/**
	 * Index of the first input row with a value equal to or greater than the lower bound of the
	 * current output row.
	 */
	int inputIndex = 0;

	public UnboundedFollowingOverFrame(
			RowType valueType,
			GeneratedAggsHandleFunction aggsHandleFunction) {
		this.valueType = valueType;
		this.aggsHandleFunction = aggsHandleFunction;
	}

	@Override
	public void open(ExecutionContext ctx) throws Exception {
		ClassLoader cl = ctx.getRuntimeContext().getUserCodeClassLoader();
		processor = aggsHandleFunction.newInstance(cl);
		processor.open(new PerKeyStateDataViewStore(ctx.getRuntimeContext()));

		this.aggsHandleFunction = null;
		this.valueSer = new RowDataSerializer(ctx.getRuntimeContext().getExecutionConfig(), valueType);
	}

	@Override
	public void prepare(ResettableExternalBuffer rows) throws Exception {
		input = rows;
		//cleanup the retired accumulators value
		processor.setAccumulators(processor.createAccumulators());
		inputIndex = 0;
	}

	RowData accumulateIterator(
			boolean bufferUpdated,
			BinaryRowData firstRow,
			ResettableExternalBuffer.BufferIterator iterator) throws Exception {
		// Only recalculate and update when the buffer changes.
		if (bufferUpdated) {
			//cleanup the retired accumulators value
			processor.setAccumulators(processor.createAccumulators());

			if (firstRow != null) {
				processor.accumulate(firstRow);
			}
			while (iterator.advanceNext()) {
				processor.accumulate(iterator.getRow());
			}
			accValue = valueSer.copy(processor.getValue());
		}
		iterator.close();
		return accValue;
	}
}
