

package org.apache.flink.table.runtime.operators.join.interval;

import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.generated.GeneratedFunction;
import org.apache.flink.table.runtime.operators.join.FlinkJoinType;
import org.apache.flink.table.runtime.typeutils.RowDataTypeInfo;

/**
 * The function to execute row(event) time interval stream inner-join.
 */
public final class RowTimeIntervalJoin extends TimeIntervalJoin {

	private final int leftTimeIdx;
	private final int rightTimeIdx;

	public RowTimeIntervalJoin(
			FlinkJoinType joinType,
			long leftLowerBound,
			long leftUpperBound,
			long allowedLateness,
			RowDataTypeInfo leftType,
			RowDataTypeInfo rightType,
			GeneratedFunction<FlatJoinFunction<RowData, RowData, RowData>> genJoinFunc,
			int leftTimeIdx,
			int rightTimeIdx) {
		super(joinType, leftLowerBound, leftUpperBound, allowedLateness, leftType, rightType, genJoinFunc);
		this.leftTimeIdx = leftTimeIdx;
		this.rightTimeIdx = rightTimeIdx;
	}

	/**
	 * Get the maximum interval between receiving a row and emitting it (as part of a joined result).
	 * This is the time interval by which watermarks need to be held back.
	 *
	 * @return the maximum delay for the outputs
	 */
	public long getMaxOutputDelay() {
		return Math.max(leftRelativeSize, rightRelativeSize) + allowedLateness;
	}

	@Override
	void updateOperatorTime(Context ctx) {
		leftOperatorTime = ctx.timerService().currentWatermark() > 0 ? ctx.timerService().currentWatermark() : 0L;
		// We may set different operator times in the future.
		rightOperatorTime = leftOperatorTime;
	}

	@Override
	long getTimeForLeftStream(Context ctx, RowData row) {
		return row.getLong(leftTimeIdx);
	}

	@Override
	long getTimeForRightStream(Context ctx, RowData row) {
		return row.getLong(rightTimeIdx);
	}

	@Override
	void registerTimer(Context ctx, long cleanupTime) {
		// Maybe we can register timers for different streams in the future.
		ctx.timerService().registerEventTimeTimer(cleanupTime);
	}
}
