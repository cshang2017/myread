package org.apache.flink.table.planner.plan.optimize.program

import org.apache.flink.table.planner.plan.`trait`.MiniBatchIntervalTrait

import org.apache.calcite.rel.RelNode

/**
  * A FlinkOptimizeProgram that does some initialization be for MiniBatch Interval inference.
  */
class FlinkMiniBatchIntervalTraitInitProgram extends FlinkOptimizeProgram[StreamOptimizeContext] {

  override def optimize(input: RelNode, context: StreamOptimizeContext): RelNode = {
    val updatedTraitSet = input.getTraitSet.plus(
      new MiniBatchIntervalTrait(context.getMiniBatchInterval))
    input.copy(updatedTraitSet, input.getInputs)
  }
}
