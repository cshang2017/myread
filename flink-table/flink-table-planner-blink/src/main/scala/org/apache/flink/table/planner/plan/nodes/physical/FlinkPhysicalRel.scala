package org.apache.flink.table.planner.plan.nodes.physical

import org.apache.flink.table.planner.plan.nodes.FlinkRelNode

import org.apache.calcite.plan.RelTraitSet
import org.apache.calcite.rel.RelNode

/**
  * Base class for flink physical relational expression.
  */
trait FlinkPhysicalRel extends FlinkRelNode {

  /**
    * Try to satisfy required traits by descendant of current node. If descendant can satisfy
    * required traits, and current node will not destroy it, then returns the new node with
    * converted inputs.
    *
    * @param requiredTraitSet required traits
    * @return A converted node which satisfy required traits by inputs node of current node.
    *         Returns None if required traits cannot be satisfied.
    */
  def satisfyTraits(requiredTraitSet: RelTraitSet): Option[RelNode] = None

}
