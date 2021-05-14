package org.apache.flink.table.planner.plan.rules.logical

import org.apache.calcite.plan.RelOptRule.{any, none, operand, some}
import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.core.{Join, JoinRelType, Values}

object FlinkPruneEmptyRules {

  /**
    * This rule is copied from Calcite's
    * [[org.apache.calcite.rel.rules.PruneEmptyRules#JOIN_RIGHT_INSTANCE]].
    *
    * Modification:
    * - Handles ANTI join specially.
    *
    * Rule that converts a [[Join]] to empty if its right child is empty.
    *
    * <p>Examples:
    *
    * <ul>
    * <li>Join(Scan(Emp), Empty, INNER) becomes Empty
    * </ul>
    */
  val JOIN_RIGHT_INSTANCE: RelOptRule = new RelOptRule(
    operand(classOf[Join],
      some(operand(classOf[RelNode], any),
        operand(classOf[Values], none))),
    "FlinkPruneEmptyRules(right)") {

    override def matches(call: RelOptRuleCall): Boolean = {
      val right: Values = call.rel(2)
      Values.IS_EMPTY.apply(right)
    }

    override def onMatch(call: RelOptRuleCall): Unit = {
      val join: Join = call.rel(0)
      join.getJoinType match {
        case JoinRelType.ANTI =>
          // "select * from emp where deptno not in (select deptno from dept where 1=0)"
          // return emp
          call.transformTo(call.builder().push(join.getLeft).build)
        case _ =>
          if (join.getJoinType.generatesNullsOnRight) {
            // "select * from emp left join dept" is not necessarily empty if dept is empty
          } else {
            call.transformTo(call.builder.push(join).empty.build)
          }
      }
    }
  }
}
