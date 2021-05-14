package org.apache.flink.table.planner.plan.nodes.exec

import org.apache.flink.table.planner.delegation.StreamPlanner
import org.apache.flink.table.planner.utils.Logging

/**
  * Base class for stream ExecNode.
  */
trait StreamExecNode[T] extends ExecNode[StreamPlanner, T] with Logging
