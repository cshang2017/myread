
package org.apache.flink.table.runtime.operators.rank;

import java.util.List;

/** ConstantRankRangeWithoutEnd is a RankRange which not specify RankEnd. */
public class ConstantRankRangeWithoutEnd implements RankRange {

	private final long rankStart;

	public ConstantRankRangeWithoutEnd(long rankStart) {
		this.rankStart = rankStart;
	}

	@Override
	public String toString(List<String> inputFieldNames) {
		return toString();
	}

	@Override
	public String toString() {
		return "rankStart=" + rankStart;
	}
}
