package org.apache.flink.table.planner.plan.optimize

import org.apache.calcite.rel.RelNode

/**
  * The query [[Optimizer]] that transforms relational expressions into
  * semantically equivalent relational expressions.
  */
trait Optimizer {

  /**
    * Generates the optimized [[RelNode]] DAG from the original relational nodes.
    * <p>NOTES: The reused node in result DAG will be converted to the same RelNode.
    *
    * @param roots the original relational nodes.
    * @return a list of RelNode represents an optimized RelNode DAG.
    */
  def optimize(roots: Seq[RelNode]): Seq[RelNode]
}
