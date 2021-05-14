
package org.apache.flink.table.planner.plan.rules.logical

import org.apache.calcite.adapter.enumerable.EnumerableTableScan
import org.apache.calcite.plan.RelOptRule.{any, operand}
import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall, RelOptRuleOperand}
import org.apache.calcite.rel.logical.LogicalTableScan

/**
  * Rule that converts an EnumerableTableScan into a LogicalTableScan.
  * We need this rule because Calcite creates an EnumerableTableScan
  * when parsing a SQL query. We convert it into a LogicalTableScan
  * so we can merge the optimization process with any plan that might be created
  * by the Table API.
  */
class EnumerableToLogicalTableScan(
    operand: RelOptRuleOperand,
    description: String) extends RelOptRule(operand, description) {

  override def onMatch(call: RelOptRuleCall): Unit = {
    val oldRel = call.rel(0).asInstanceOf[EnumerableTableScan]
    val table = oldRel.getTable
    val newRel = LogicalTableScan.create(oldRel.getCluster, table)
    call.transformTo(newRel)
  }
}

object EnumerableToLogicalTableScan {
  val INSTANCE = new EnumerableToLogicalTableScan(
    operand(classOf[EnumerableTableScan], any),
    "EnumerableToLogicalTableScan")
}
