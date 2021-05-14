package org.apache.flink.table.runtime.operators.over.frame;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.context.ExecutionContext;
import org.apache.flink.table.runtime.dataview.PerKeyStateDataViewStore;
import org.apache.flink.table.runtime.generated.AggsHandleFunction;
import org.apache.flink.table.runtime.generated.GeneratedAggsHandleFunction;
import org.apache.flink.table.runtime.util.ResettableExternalBuffer;

/**
 * The insensitive window frame calculates the statements which shouldn't care the window frame,
 * for example RANK/DENSE_RANK/PERCENT_RANK/CUME_DIST/ROW_NUMBER.
 */
public class InsensitiveOverFrame implements OverWindowFrame {

	private GeneratedAggsHandleFunction aggsHandleFunction;
	private AggsHandleFunction processor;

	public InsensitiveOverFrame(
			GeneratedAggsHandleFunction aggsHandleFunction) {
		this.aggsHandleFunction = aggsHandleFunction;
	}

	@Override
	public void open(ExecutionContext ctx) throws Exception {
		processor = aggsHandleFunction.newInstance(ctx.getRuntimeContext().getUserCodeClassLoader());
		processor.open(new PerKeyStateDataViewStore(ctx.getRuntimeContext()));

		this.aggsHandleFunction = null;
	}

	@Override
	public void prepare(ResettableExternalBuffer rows) throws Exception {
		//reset the accumulator value
		processor.setAccumulators(processor.createAccumulators());
	}

	@Override
	public RowData process(int index, RowData current) throws Exception {
		processor.accumulate(current);
		return processor.getValue();
	}
}
