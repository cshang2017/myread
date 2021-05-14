package org.apache.flink.table.planner.plan.nodes.exec

import org.apache.flink.runtime.operators.DamBehavior
import org.apache.flink.table.planner.delegation.BatchPlanner
import org.apache.flink.table.planner.utils.Logging

/**
  * Base class for batch ExecNode.
  */
trait BatchExecNode[T] extends ExecNode[BatchPlanner, T] with Logging {

  /**
    * Returns [[DamBehavior]] of this node.
    */
  def getDamBehavior: DamBehavior

}
