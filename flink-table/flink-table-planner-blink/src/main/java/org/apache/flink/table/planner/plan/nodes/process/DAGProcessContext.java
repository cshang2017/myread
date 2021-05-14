package org.apache.flink.table.planner.plan.nodes.process;

import org.apache.flink.table.planner.delegation.PlannerBase;

/**
 * Context for processors to process dag.
 */
public class DAGProcessContext {

	private final PlannerBase planner;

	public DAGProcessContext(PlannerBase planner) {
		this.planner = planner;
	}

	/**
	 * Gets {@link PlannerBase}, {@link org.apache.flink.table.planner.delegation.BatchPlanner} for batch job.
	 * and {@link org.apache.flink.table.planner.delegation.StreamPlanner} for stream job.
	 */
	public PlannerBase getPlanner() {
		return planner;
	}

}
