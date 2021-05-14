package org.apache.flink.table.runtime.operators.join;

/**
 * Join type for hash table.
 */
public enum HashJoinType {

	INNER,
	BUILD_OUTER,
	PROBE_OUTER,
	FULL_OUTER,
	SEMI,
	ANTI,
	BUILD_LEFT_SEMI,
	BUILD_LEFT_ANTI;

	public boolean isBuildOuter() {
		return this.equals(BUILD_OUTER) || this.equals(FULL_OUTER);
	}

	public boolean isProbeOuter() {
		return this.equals(PROBE_OUTER) || this.equals(FULL_OUTER);
	}

	public boolean buildLeftSemiOrAnti() {
		return this.equals(BUILD_LEFT_SEMI) || this.equals(BUILD_LEFT_ANTI);
	}

	public boolean needSetProbed() {
		return isBuildOuter() || buildLeftSemiOrAnti();
	}

	public static HashJoinType of(boolean leftIsBuild, boolean leftOuter, boolean rightOuter) {
		if (leftOuter && rightOuter) {
			return FULL_OUTER;
		} else if (leftOuter) {
			return leftIsBuild ? BUILD_OUTER : PROBE_OUTER;
		} else if (rightOuter) {
			return leftIsBuild ? PROBE_OUTER : BUILD_OUTER;
		} else {
			return INNER;
		}
	}

	public static HashJoinType of(boolean leftIsBuild, boolean leftOuter, boolean rightOuter,
			boolean isSemi, boolean isAnti) {
		if (leftOuter && rightOuter) {
			return FULL_OUTER;
		} else if (leftOuter) {
			return leftIsBuild ? BUILD_OUTER : PROBE_OUTER;
		} else if (rightOuter) {
			return leftIsBuild ? PROBE_OUTER : BUILD_OUTER;
		} else if (isSemi) {
			return leftIsBuild ? BUILD_LEFT_SEMI : SEMI;
		} else if (isAnti) {
			return leftIsBuild ? BUILD_LEFT_ANTI : ANTI;
		} else {
			return INNER;
		}
	}
}
