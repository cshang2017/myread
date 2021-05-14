package org.apache.flink.table.runtime.operators.sort;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.data.util.RowDataUtil;
import org.apache.flink.table.runtime.generated.GeneratedRecordComparator;
import org.apache.flink.table.runtime.generated.RecordComparator;
import org.apache.flink.table.runtime.operators.TableStreamOperator;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.runtime.typeutils.RowDataTypeInfo;
import org.apache.flink.table.runtime.util.StreamRecordCollector;
import org.apache.flink.types.RowKind;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class StreamSortOperator extends TableStreamOperator<RowData> implements
		OneInputStreamOperator<RowData, RowData> {

	private final RowDataTypeInfo inputRowType;
	private GeneratedRecordComparator gComparator;
	private transient RecordComparator comparator;
	private transient RowDataSerializer rowDataSerializer;
	private transient StreamRecordCollector<RowData> collector;

	private transient ListState<Tuple2<RowData, Long>> bufferState;

	// inputBuffer buffers all input elements, key is RowData, value is appear times.
	private transient HashMap<RowData, Long> inputBuffer;

	public StreamSortOperator(RowDataTypeInfo inputRowType, GeneratedRecordComparator gComparator) {
		this.inputRowType = inputRowType;
		this.gComparator = gComparator;
	}

	@Override
	public void open() throws Exception {
		super.open();

		ExecutionConfig executionConfig = getExecutionConfig();
		this.rowDataSerializer = inputRowType.createSerializer(executionConfig);

		comparator = gComparator.newInstance(getContainingTask().getUserCodeClassLoader());
		gComparator = null;

		this.collector = new StreamRecordCollector<>(output);
		this.inputBuffer = new HashMap<>();

		// restore state
		if (bufferState != null) {
			bufferState.get().forEach((Tuple2<RowData, Long> input) -> inputBuffer.put(input.f0, input.f1));
		}
	}

	@Override
	public void processElement(StreamRecord<RowData> element) throws Exception {
		RowData originalInput = element.getValue();
		BinaryRowData input = rowDataSerializer.toBinaryRow(originalInput).copy();
		input.setRowKind(RowKind.INSERT); // erase RowKind for state updating
		long count = inputBuffer.getOrDefault(input, 0L);
		if (RowDataUtil.isAccumulateMsg(originalInput)) {
			inputBuffer.put(input, count + 1);
		} else {
			if (count == 0L) {
				throw new RuntimeException("RowData not exist!");
			} else if (count == 1) {
				inputBuffer.remove(input);
			} else {
				inputBuffer.put(input, count - 1);
			}
		}
	}

	@Override
	public void initializeState(StateInitializationContext context) throws Exception {
		super.initializeState(context);
		TupleTypeInfo<Tuple2<RowData, Long>> tupleType = new TupleTypeInfo<>(inputRowType, Types.LONG);
		this.bufferState = context.getOperatorStateStore()
				.getListState(new ListStateDescriptor<>("localBufferState", tupleType));
	}

	@Override
	public void snapshotState(StateSnapshotContext context) throws Exception {
		super.snapshotState(context);
		// clear state first
		bufferState.clear();

		List<Tuple2<RowData, Long>> dataToFlush = new ArrayList<>(inputBuffer.size());
		inputBuffer.forEach((key, value) -> dataToFlush.add(Tuple2.of(key, value)));

		// batch put
		bufferState.addAll(dataToFlush);
	}

	@Override
	public void close() throws Exception {

		// BoundedOneInput can not coexistence with checkpoint, so we emit output in close.
		if (!inputBuffer.isEmpty()) {
			List<RowData> rowsSet = new ArrayList<>();
			rowsSet.addAll(inputBuffer.keySet());
			// sort the rows
			rowsSet.sort(comparator);

			// Emit the rows in order
			rowsSet.forEach((RowData row) -> {
				long count = inputBuffer.get(row);
				for (int i = 1; i <= count; i++) {
					collector.collect(row);
				}
			});
		}
		super.close();
	}
}
