package org.apache.flink.table.planner.plan.utils;

import org.apache.flink.table.runtime.operators.join.FlinkJoinType;

import org.apache.calcite.rel.core.JoinRelType;

/**
 * Utility for {@link FlinkJoinType}.
 */
public class JoinTypeUtil {

	/**
	 * Converts {@link JoinRelType} to {@link FlinkJoinType}.
	 */
	public static FlinkJoinType getFlinkJoinType(JoinRelType joinRelType) {
		switch (joinRelType) {
			case INNER:
				return FlinkJoinType.INNER;
			case LEFT:
				return FlinkJoinType.LEFT;
			case RIGHT:
				return FlinkJoinType.RIGHT;
			case FULL:
				return FlinkJoinType.FULL;
			case SEMI:
				return FlinkJoinType.SEMI;
			case ANTI:
				return FlinkJoinType.ANTI;
			default:
				throw new IllegalArgumentException("invalid: " + joinRelType);
		}
	}

}
