

package org.apache.flink.table.runtime.operators.over.frame;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.runtime.context.ExecutionContext;
import org.apache.flink.table.runtime.dataview.PerKeyStateDataViewStore;
import org.apache.flink.table.runtime.generated.AggsHandleFunction;
import org.apache.flink.table.runtime.generated.GeneratedAggsHandleFunction;
import org.apache.flink.table.runtime.util.ResettableExternalBuffer;

/**
 * The UnboundedPreceding window frame.
 * See {@link RowUnboundedPrecedingOverFrame} and {@link RangeUnboundedPrecedingOverFrame}.
 */
public abstract class UnboundedPrecedingOverFrame implements OverWindowFrame {

	private GeneratedAggsHandleFunction aggsHandleFunction;

	AggsHandleFunction processor;
	RowData accValue;

	/**
	 * An iterator over the input.
	 */
	ResettableExternalBuffer.BufferIterator inputIterator;

	/** The next row from `input`. */
	BinaryRowData nextRow;

	public UnboundedPrecedingOverFrame(GeneratedAggsHandleFunction aggsHandleFunction) {
		this.aggsHandleFunction = aggsHandleFunction;
	}

	@Override
	public void open(ExecutionContext ctx) throws Exception {
		ClassLoader cl = ctx.getRuntimeContext().getUserCodeClassLoader();
		processor = aggsHandleFunction.newInstance(cl);
		processor.open(new PerKeyStateDataViewStore(ctx.getRuntimeContext()));
		this.aggsHandleFunction = null;
	}

	@Override
	public void prepare(ResettableExternalBuffer rows) throws Exception {
		if (inputIterator != null) {
			inputIterator.close();
		}
		inputIterator = rows.newIterator();
		if (inputIterator.advanceNext()) {
			nextRow = inputIterator.getRow().copy();
		}
		//reset the accumulators value
		processor.setAccumulators(processor.createAccumulators());
	}
}
