

package org.apache.flink.table.planner.plan.rules.logical

import org.apache.flink.table.planner.plan.utils.SetOpRewriteUtil.generateEqualsCondition

import org.apache.calcite.plan.RelOptRule.{any, operand}
import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall}
import org.apache.calcite.rel.core.{Aggregate, Intersect, Join, JoinRelType, RelFactories}

import scala.collection.JavaConversions._

/**
  * Planner rule that replaces distinct [[Intersect]] with
  * a distinct [[Aggregate]] on a SEMI [[Join]].
  *
  * Only handle the case of input size 2.
  */
class ReplaceIntersectWithSemiJoinRule extends RelOptRule(
  operand(classOf[Intersect], any),
  RelFactories.LOGICAL_BUILDER,
  "ReplaceIntersectWithSemiJoinRule") {

  override def matches(call: RelOptRuleCall): Boolean = {
    val intersect: Intersect = call.rel(0)
    !intersect.all && intersect.getInputs.size() == 2
  }

  override def onMatch(call: RelOptRuleCall): Unit = {
    val intersect: Intersect = call.rel(0)
    val left = intersect.getInput(0)
    val right = intersect.getInput(1)

    val relBuilder = call.builder
    val keys = 0 until left.getRowType.getFieldCount
    val conditions = generateEqualsCondition(relBuilder, left, right, keys)

    relBuilder.push(left)
    relBuilder.push(right)
    relBuilder.join(JoinRelType.SEMI, conditions).aggregate(relBuilder.groupKey(keys: _*))
    val rel = relBuilder.build()
    call.transformTo(rel)
  }
}

object ReplaceIntersectWithSemiJoinRule {
  val INSTANCE: RelOptRule = new ReplaceIntersectWithSemiJoinRule
}
