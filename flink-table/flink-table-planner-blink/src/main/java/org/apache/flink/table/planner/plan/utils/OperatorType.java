package org.apache.flink.table.planner.plan.utils;

import org.apache.flink.table.api.config.ExecutionConfigOptions;

/**
 * Some dedicated operator type which is used in
 * {@link ExecutionConfigOptions#TABLE_EXEC_DISABLED_OPERATORS}.
 */
public enum OperatorType {

	NestedLoopJoin,

	ShuffleHashJoin,

	BroadcastHashJoin,

	SortMergeJoin,

	HashAgg,

	SortAgg
}
