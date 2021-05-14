package org.apache.flink.table.planner.plan.optimize.program

import org.apache.calcite.rel.RelNode

/**
  * Likes [[org.apache.calcite.tools.Program]], FlinkOptimizeProgram transforms a relational
  * expression into another relational expression.
  *
  * @tparam OC OptimizeContext
  */
trait FlinkOptimizeProgram[OC <: FlinkOptimizeContext] {

  /**
    * Transforms a relational expression into another relational expression.
    */
  def optimize(root: RelNode, context: OC): RelNode

}
