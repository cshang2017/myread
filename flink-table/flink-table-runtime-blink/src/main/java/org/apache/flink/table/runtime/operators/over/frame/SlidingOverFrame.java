
package org.apache.flink.table.runtime.operators.over.frame;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.runtime.context.ExecutionContext;
import org.apache.flink.table.runtime.dataview.PerKeyStateDataViewStore;
import org.apache.flink.table.runtime.generated.AggsHandleFunction;
import org.apache.flink.table.runtime.generated.GeneratedAggsHandleFunction;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.runtime.util.ResettableExternalBuffer;
import org.apache.flink.table.types.logical.RowType;

import java.util.ArrayDeque;

/**
 * The sliding window frame.
 * See {@link RowSlidingOverFrame} and {@link RangeSlidingOverFrame}.
 */
public abstract class SlidingOverFrame implements OverWindowFrame {

	private final RowType inputType;
	private final RowType valueType;
	private GeneratedAggsHandleFunction aggsHandleFunction;

	private transient AggsHandleFunction processor;
	transient RowDataSerializer inputSer;
	private transient RowDataSerializer valueSer;

	transient ResettableExternalBuffer.BufferIterator inputIterator;

	/** The next row from `input`. */
	transient BinaryRowData nextRow;

	/** The rows within current sliding window. */
	transient ArrayDeque<RowData> buffer;

	private transient RowData accValue;

	public SlidingOverFrame(
			RowType inputType,
			RowType valueType,
			GeneratedAggsHandleFunction aggsHandleFunction) {
		this.inputType = inputType;
		this.valueType = valueType;
		this.aggsHandleFunction = aggsHandleFunction;
	}

	@Override
	public void open(ExecutionContext ctx) throws Exception {
		ExecutionConfig conf = ctx.getRuntimeContext().getExecutionConfig();
		this.inputSer = new RowDataSerializer(conf, inputType);
		this.valueSer = new RowDataSerializer(conf, valueType);

		ClassLoader cl = ctx.getRuntimeContext().getUserCodeClassLoader();
		processor = aggsHandleFunction.newInstance(cl);
		processor.open(new PerKeyStateDataViewStore(ctx.getRuntimeContext()));
		buffer = new ArrayDeque<>();
		this.aggsHandleFunction = null;
	}

	@Override
	public void prepare(ResettableExternalBuffer rows) throws Exception {
		if (inputIterator != null) {
			inputIterator.close();
		}
		inputIterator = rows.newIterator();
		nextRow = OverWindowFrame.getNextOrNull(inputIterator);
		buffer.clear();
		//cleanup the retired accumulators value
		processor.setAccumulators(processor.createAccumulators());
	}

	RowData accumulateBuffer(boolean bufferUpdated) throws Exception {
		// Only recalculate and update when the buffer changes.
		if (bufferUpdated) {
			//cleanup the retired accumulators value
			processor.setAccumulators(processor.createAccumulators());
			for (RowData row : buffer) {
				processor.accumulate(row);
			}
			accValue = valueSer.copy(processor.getValue());
		}
		return accValue;
	}
}
