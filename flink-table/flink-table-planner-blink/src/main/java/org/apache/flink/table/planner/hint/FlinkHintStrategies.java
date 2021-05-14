package org.apache.flink.table.planner.hint;

import org.apache.calcite.rel.hint.HintPredicates;
import org.apache.calcite.rel.hint.HintStrategy;
import org.apache.calcite.rel.hint.HintStrategyTable;
import org.apache.calcite.util.Litmus;

/**
 * A collection of Flink style {@link HintStrategy}s.
 */
public abstract class FlinkHintStrategies {

	/** Customize the {@link HintStrategyTable} which
	 * contains hint strategies supported by Flink. */
	public static HintStrategyTable createHintStrategyTable() {
		return HintStrategyTable.builder()
				// Configure to always throw when we encounter any hint errors
				// (either the non-registered hint or the hint format).
				.errorHandler(Litmus.THROW)
				.hintStrategy(
						FlinkHints.HINT_NAME_OPTIONS,
						HintStrategy.builder(HintPredicates.TABLE_SCAN)
								.optionChecker((hint, errorHandler) ->
										errorHandler.check(hint.kvOptions.size() > 0,
										"Hint [{}] only support non empty key value options",
										hint.hintName))
								.build())
				.build();
	}
}
