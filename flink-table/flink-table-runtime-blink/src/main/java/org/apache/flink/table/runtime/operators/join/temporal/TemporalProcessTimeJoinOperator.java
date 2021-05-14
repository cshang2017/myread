package org.apache.flink.table.runtime.operators.join.temporal;

import org.apache.flink.api.common.functions.util.FunctionUtils;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.streaming.api.operators.InternalTimer;
import org.apache.flink.streaming.api.operators.TimestampedCollector;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.JoinedRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.util.RowDataUtil;
import org.apache.flink.table.runtime.generated.GeneratedJoinCondition;
import org.apache.flink.table.runtime.generated.JoinCondition;
import org.apache.flink.table.runtime.typeutils.RowDataTypeInfo;

/**
 * The operator to temporal join a stream on processing time.
 */
public class TemporalProcessTimeJoinOperator
	extends BaseTwoInputStreamOperatorWithStateRetention {

	private final RowDataTypeInfo rightType;
	private final GeneratedJoinCondition generatedJoinCondition;

	private transient ValueState<RowData> rightState;
	private transient JoinCondition joinCondition;

	private transient JoinedRowData outRow;
	private transient TimestampedCollector<RowData> collector;

	public TemporalProcessTimeJoinOperator(
			RowDataTypeInfo rightType,
			GeneratedJoinCondition generatedJoinCondition,
			long minRetentionTime,
			long maxRetentionTime) {
		super(minRetentionTime, maxRetentionTime);
		this.rightType = rightType;
		this.generatedJoinCondition = generatedJoinCondition;
	}

	@Override
	public void open() throws Exception {
		this.joinCondition = generatedJoinCondition.newInstance(getRuntimeContext().getUserCodeClassLoader());
		FunctionUtils.setFunctionRuntimeContext(joinCondition, getRuntimeContext());
		FunctionUtils.openFunction(joinCondition, new Configuration());

		ValueStateDescriptor<RowData> rightStateDesc = new ValueStateDescriptor<>("right", rightType);
		this.rightState = getRuntimeContext().getState(rightStateDesc);
		this.collector = new TimestampedCollector<>(output);
		this.outRow = new JoinedRowData();
		// consider watermark from left stream only.
		super.processWatermark2(Watermark.MAX_WATERMARK);
	}

	@Override
	public void processElement1(StreamRecord<RowData> element) throws Exception {
		RowData rightSideRow = rightState.value();
		if (rightSideRow == null) {
			return;
		}

		RowData leftSideRow = element.getValue();
		if (joinCondition.apply(leftSideRow, rightSideRow)) {
			outRow.setRowKind(leftSideRow.getRowKind());
			outRow.replace(leftSideRow, rightSideRow);
			collector.collect(outRow);
		}
		registerProcessingCleanupTimer();
	}

	@Override
	public void processElement2(StreamRecord<RowData> element) throws Exception {
		if (RowDataUtil.isAccumulateMsg(element.getValue())) {
			rightState.update(element.getValue());
			registerProcessingCleanupTimer();
		} else {
			rightState.clear();
			cleanupLastTimer();
		}
	}

	@Override
	public void close() throws Exception {
		FunctionUtils.closeFunction(joinCondition);
	}

	/**
	 * The method to be called when a cleanup timer fires.
	 *
	 * @param time The timestamp of the fired timer.
	 */
	@Override
	public void cleanupState(long time) {
		rightState.clear();
	}

	/**
	 * Invoked when an event-time timer fires.
	 */
	@Override
	public void onEventTime(InternalTimer<Object, VoidNamespace> timer) throws Exception {
	}
}
