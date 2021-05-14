

package org.apache.flink.table.planner.utils;

import org.apache.flink.table.api.config.OptimizerConfigOptions;

/**
 * Aggregate phase strategy which could be specified in {@link OptimizerConfigOptions#TABLE_OPTIMIZER_AGG_PHASE_STRATEGY}.
 */
public enum AggregatePhaseStrategy {

	/**
	 * No special enforcer for aggregate stage. Whether to choose two stage aggregate or one stage aggregate depends on cost.
	 */
	AUTO,

	/**
	 * Enforce to use one stage aggregate which only has CompleteGlobalAggregate.
	 */
	ONE_PHASE,

	/**
	 * Enforce to use two stage aggregate which has localAggregate and globalAggregate.
	 * NOTE: If aggregate call does not support split into two phase, still use one stage aggregate.
	 */
	TWO_PHASE
}
