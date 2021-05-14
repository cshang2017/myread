package org.apache.flink.table.runtime.operators.join;

/**
 * Join type for join.
 */
public enum FlinkJoinType {
	INNER, LEFT, RIGHT, FULL, SEMI, ANTI;

	public boolean isOuter() {
		switch (this) {
			case LEFT:
			case RIGHT:
			case FULL:
				return true;
			default:
				return false;
		}
	}

	public boolean isLeftOuter() {
		switch (this) {
			case LEFT:
			case FULL:
				return true;
			default:
				return false;
		}
	}

	public boolean isRightOuter() {
		switch (this) {
			case RIGHT:
			case FULL:
				return true;
			default:
				return false;
		}
	}

	@Override
	public String toString() {
		switch (this) {
			case INNER:
				return "InnerJoin";
			case LEFT:
				return "LeftOuterJoin";
			case RIGHT:
				return "RightOuterJoin";
			case FULL:
				return "FullOuterJoin";
			case SEMI:
				return "LeftSemiJoin";
			case ANTI:
				return "LeftAntiJoin";
			default:
				throw new IllegalArgumentException("Invalid join type: " + name());
		}
	}
}
