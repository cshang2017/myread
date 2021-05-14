package org.apache.flink.table.planner.plan.cost

import org.apache.calcite.plan.RelOptCost

/**
  * This class is based on Apache Calcite's [[org.apache.calcite.plan.volcano.VolcanoCost#Factory]].
  */
class FlinkCostFactory extends FlinkCostFactoryBase {

  override def makeCost(
      rowCount: Double,
      cpu: Double,
      io: Double,
      network: Double,
      memory: Double): RelOptCost = {
    new FlinkCost(rowCount, cpu, io, network, memory)
  }

  override def makeCost(dRows: Double, dCpu: Double, dIo: Double): RelOptCost = {
    new FlinkCost(dRows, dCpu, dIo, 0.0, 0.0)
  }

  override def makeHugeCost: RelOptCost = FlinkCost.Huge

  override def makeInfiniteCost: RelOptCost = FlinkCost.Infinity

  override def makeTinyCost: RelOptCost = FlinkCost.Tiny

  override def makeZeroCost: RelOptCost = FlinkCost.Zero

}
