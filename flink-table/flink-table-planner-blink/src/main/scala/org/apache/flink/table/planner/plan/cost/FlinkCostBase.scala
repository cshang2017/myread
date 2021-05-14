package org.apache.flink.table.planner.plan.cost

import org.apache.calcite.plan.RelOptCost

/**
  * A [[RelOptCost]] that extends network cost and memory cost.
  */
trait FlinkCostBase extends RelOptCost {

  /**
    * @return usage of network resources
    */
  def getNetwork: Double

  /**
    * @return usage of memory resources
    */
  def getMemory: Double
}
