

package org.apache.flink.table.runtime.operators.join.interval;

import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.generated.GeneratedFunction;
import org.apache.flink.table.runtime.operators.join.FlinkJoinType;
import org.apache.flink.table.runtime.typeutils.RowDataTypeInfo;

/**
 * The function to execute processing time interval stream inner-join.
 */
public final class ProcTimeIntervalJoin extends TimeIntervalJoin {


	public ProcTimeIntervalJoin(
			FlinkJoinType joinType,
			long leftLowerBound,
			long leftUpperBound,
			RowDataTypeInfo leftType,
			RowDataTypeInfo rightType,
			GeneratedFunction<FlatJoinFunction<RowData, RowData, RowData>> genJoinFunc) {
		super(joinType, leftLowerBound, leftUpperBound, 0L, leftType, rightType, genJoinFunc);
	}

	@Override
	void updateOperatorTime(Context ctx) {
		leftOperatorTime = ctx.timerService().currentProcessingTime();
		rightOperatorTime = leftOperatorTime;
	}

	@Override
	long getTimeForLeftStream(Context ctx, RowData row) {
		return leftOperatorTime;
	}

	@Override
	long getTimeForRightStream(Context ctx, RowData row) {
		return rightOperatorTime;
	}

	@Override
	void registerTimer(Context ctx, long cleanupTime) {
		ctx.timerService().registerProcessingTimeTimer(cleanupTime);
	}
}
