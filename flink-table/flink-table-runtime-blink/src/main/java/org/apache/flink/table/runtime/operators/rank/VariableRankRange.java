package org.apache.flink.table.runtime.operators.rank;

import java.util.List;

/**
 * changing rank limit depends on input.
 */
public class VariableRankRange implements RankRange {

	private int rankEndIndex;

	public VariableRankRange(int rankEndIndex) {
		this.rankEndIndex = rankEndIndex;
	}

	public int getRankEndIndex() {
		return rankEndIndex;
	}

	@Override
	public String toString(List<String> inputFieldNames) {
		return "rankEnd=" + inputFieldNames.get(rankEndIndex);
	}

	@Override
	public String toString() {
		return "rankEnd=$" + rankEndIndex;
	}

}
