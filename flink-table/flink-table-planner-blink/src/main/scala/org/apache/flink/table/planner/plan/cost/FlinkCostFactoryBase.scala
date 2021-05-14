

package org.apache.flink.table.planner.plan.cost

import org.apache.calcite.plan.{RelOptCost, RelOptCostFactory}

/**
  * A [[RelOptCostFactory]] that adds makeCost methods with network and memory parameters.
  */
trait FlinkCostFactoryBase extends RelOptCostFactory {

  def makeCost(
    rowCount: Double,
    cpu: Double,
    io: Double,
    network: Double,
    memory: Double): RelOptCost

}
