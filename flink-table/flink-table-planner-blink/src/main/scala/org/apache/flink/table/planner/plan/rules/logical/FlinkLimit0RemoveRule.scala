package org.apache.flink.table.planner.plan.rules.logical

import org.apache.calcite.plan.RelOptRule.{any, operand}
import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall}
import org.apache.calcite.rel.core.Sort
import org.apache.calcite.rex.RexLiteral

/**
  * Planner rule that rewrites `limit 0` to empty [[org.apache.calcite.rel.core.Values]].
  */
class FlinkLimit0RemoveRule extends RelOptRule(
  operand(classOf[Sort], any()),
  "FlinkLimit0RemoveRule") {

  override def matches(call: RelOptRuleCall): Boolean = {
    val sort: Sort = call.rel(0)
    sort.fetch != null && RexLiteral.intValue(sort.fetch) == 0
  }

  override def onMatch(call: RelOptRuleCall): Unit = {
    val sort: Sort = call.rel(0)
    val emptyValues = call.builder().values(sort.getRowType).build()
    call.transformTo(emptyValues)

    // New plan is absolutely better than old plan.
    call.getPlanner.setImportance(sort, 0.0)
  }
}

object FlinkLimit0RemoveRule {
  val INSTANCE = new FlinkLimit0RemoveRule
}
