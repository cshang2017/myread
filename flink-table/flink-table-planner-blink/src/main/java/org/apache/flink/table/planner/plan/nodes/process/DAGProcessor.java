package org.apache.flink.table.planner.plan.nodes.process;

import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;

import java.util.List;

/**
 * DAGProcess plugin, use it can set resource of dag or change other node info.
 */
public interface DAGProcessor {

	/**
	 * Given a dag, process it and return the result dag.
	 */
	List<ExecNode<?, ?>> process(List<ExecNode<?, ?>> sinkNodes, DAGProcessContext context);
}
