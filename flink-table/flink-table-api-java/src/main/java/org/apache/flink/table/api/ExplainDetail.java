package org.apache.flink.table.api;

import org.apache.flink.annotation.PublicEvolving;

/**
 * ExplainDetail defines the types of details for explain result.
 */
@PublicEvolving
public enum ExplainDetail {
	/**
	 * The cost information on physical rel node estimated by optimizer.
	 * e.g. TableSourceScan(..., cumulative cost = {1.0E8 rows, 1.0E8 cpu, 2.4E9 io, 0.0 network, 0.0 memory}
	 */
	ESTIMATED_COST,

	/**
	 * The changelog mode produced by a physical rel node.
	 * e.g. GroupAggregate(..., changelogMode=[I,UA,D])
	 */
	CHANGELOG_MODE
}
