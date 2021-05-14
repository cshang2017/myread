package org.apache.flink.table.planner.plan.rules.logical

import org.apache.flink.table.planner.plan.utils.SetOpRewriteUtil.generateEqualsCondition

import org.apache.calcite.plan.RelOptRule.{any, operand}
import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall}
import org.apache.calcite.rel.core._

import scala.collection.JavaConversions._

/**
  * Planner rule that replaces distinct [[Minus]] (SQL keyword: EXCEPT) with
  * a distinct [[Aggregate]] on an ANTI [[Join]].
  *
  * Only handle the case of input size 2.
  */
class ReplaceMinusWithAntiJoinRule extends RelOptRule(
  operand(classOf[Minus], any),
  RelFactories.LOGICAL_BUILDER,
  "ReplaceMinusWithAntiJoinRule") {

  override def matches(call: RelOptRuleCall): Boolean = {
    val minus: Minus = call.rel(0)
    !minus.all && minus.getInputs.size() == 2
  }

  override def onMatch(call: RelOptRuleCall): Unit = {
    val minus: Minus = call.rel(0)
    val left = minus.getInput(0)
    val right = minus.getInput(1)

    val relBuilder = call.builder
    val keys = 0 until left.getRowType.getFieldCount
    val conditions = generateEqualsCondition(relBuilder, left, right, keys)

    relBuilder.push(left)
    relBuilder.push(right)
    relBuilder.join(JoinRelType.ANTI, conditions).aggregate(relBuilder.groupKey(keys: _*))
    val rel = relBuilder.build()
    call.transformTo(rel)
  }
}

object ReplaceMinusWithAntiJoinRule {
  val INSTANCE: RelOptRule = new ReplaceMinusWithAntiJoinRule
}
