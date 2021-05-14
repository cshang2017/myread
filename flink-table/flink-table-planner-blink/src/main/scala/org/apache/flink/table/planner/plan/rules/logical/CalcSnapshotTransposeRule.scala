
package org.apache.flink.table.planner.plan.rules.logical

import org.apache.flink.table.planner.plan.nodes.logical.{FlinkLogicalCalc, FlinkLogicalSnapshot}

import org.apache.calcite.plan.RelOptRule.{any, operand}
import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall}
import org.apache.calcite.rex.RexOver

/**
  * Transpose [[FlinkLogicalCalc]] past into [[FlinkLogicalSnapshot]].
  */
class CalcSnapshotTransposeRule extends RelOptRule(
  operand(classOf[FlinkLogicalCalc],
    operand(classOf[FlinkLogicalSnapshot], any())),
  "CalcSnapshotTransposeRule") {

  override def matches(call: RelOptRuleCall): Boolean = {
    val calc = call.rel[FlinkLogicalCalc](0)
    // Don't push a calc which contains windowed aggregates into a snapshot for now.
    !RexOver.containsOver(calc.getProgram)
  }

  override def onMatch(call: RelOptRuleCall): Unit = {
    val calc = call.rel[FlinkLogicalCalc](0)
    val snapshot = call.rel[FlinkLogicalSnapshot](1)
    val newClac = calc.copy(calc.getTraitSet, snapshot.getInputs)
    val newSnapshot = snapshot.copy(snapshot.getTraitSet, newClac, snapshot.getPeriod)
    call.transformTo(newSnapshot)
  }
}

object CalcSnapshotTransposeRule {
  val INSTANCE = new CalcSnapshotTransposeRule
}
