package org.apache.flink.table.runtime.operators.rank;

import java.io.Serializable;
import java.util.List;

/**
 * RankRange for Rank, including following 3 types :
 * ConstantRankRange, ConstantRankRangeWithoutEnd, VariableRankRange.
 */
public interface RankRange extends Serializable {

	String toString(List<String> inputFieldNames);

}
