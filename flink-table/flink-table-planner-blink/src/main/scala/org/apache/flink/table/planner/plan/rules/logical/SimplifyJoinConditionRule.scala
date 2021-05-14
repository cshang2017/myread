package org.apache.flink.table.planner.plan.rules.logical

import org.apache.flink.table.planner.plan.utils.FlinkRexUtil

import org.apache.calcite.plan.RelOptRule.{any, operand}
import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall}
import org.apache.calcite.rel.logical.LogicalJoin
import org.apache.calcite.rex._

/**
  * Planner rule that apply various simplifying transformations on join condition.
  * e.g.
  * reduce same expressions: a=b AND b=a -> a=b,
  * simplify boolean expressions: x = 1 AND FALSE -> FALSE,
  * simplify cast expressions: CAST('123' as integer) -> 123
  */
class SimplifyJoinConditionRule
  extends RelOptRule(
    operand(classOf[LogicalJoin], any()),
    "SimplifyJoinConditionRule") {

  override def onMatch(call: RelOptRuleCall): Unit = {
    val join: LogicalJoin = call.rel(0)
    val condition = join.getCondition

    if (join.getCondition.isAlwaysTrue) {
      return
    }

    val simpleCondExp = FlinkRexUtil.simplify(join.getCluster.getRexBuilder, condition)
    val newCondExp = RexUtil.pullFactors(join.getCluster.getRexBuilder, simpleCondExp)

    if (newCondExp.toString.equals(condition.toString)) {
      return
    }

    val newJoin = join.copy(
      join.getTraitSet,
      newCondExp,
      join.getLeft,
      join.getRight,
      join.getJoinType,
      join.isSemiJoinDone)

    call.transformTo(newJoin)
    call.getPlanner.setImportance(join, 0.0)
  }
}

object SimplifyJoinConditionRule {
  val INSTANCE = new SimplifyJoinConditionRule
}
