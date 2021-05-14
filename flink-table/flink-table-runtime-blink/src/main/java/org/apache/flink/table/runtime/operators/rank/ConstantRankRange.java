package org.apache.flink.table.runtime.operators.rank;

import java.util.List;

/** rankStart and rankEnd are inclusive, rankStart always start from one. */
public class ConstantRankRange implements RankRange {

	private long rankStart;
	private long rankEnd;

	public ConstantRankRange(long rankStart, long rankEnd) {
		this.rankStart = rankStart;
		this.rankEnd = rankEnd;
	}

	public long getRankStart() {
		return rankStart;
	}

	public long getRankEnd() {
		return rankEnd;
	}

	@Override
	public String toString(List<String> inputFieldNames) {
		return toString();
	}

	@Override
	public String toString() {
		return "rankStart=" + rankStart + ", rankEnd=" + rankEnd;
	}

}
