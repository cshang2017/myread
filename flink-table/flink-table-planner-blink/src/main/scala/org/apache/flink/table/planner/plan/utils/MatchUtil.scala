

package org.apache.flink.table.planner.plan.utils

import org.apache.flink.table.api.ValidationException
import org.apache.flink.table.planner.codegen.MatchCodeGenerator.ALL_PATTERN_VARIABLE

import org.apache.calcite.rex.{RexCall, RexNode, RexPatternFieldRef}

import _root_.scala.collection.JavaConverters._

object MatchUtil {

  class AggregationPatternVariableFinder extends RexDefaultVisitor[Option[String]] {

    override def visitPatternFieldRef(patternFieldRef: RexPatternFieldRef): Option[String] = Some(
      patternFieldRef.getAlpha)

    override def visitCall(call: RexCall): Option[String] = {
      if (call.operands.size() == 0) {
        Some(ALL_PATTERN_VARIABLE)
      } else {
        call.operands.asScala.map(n => n.accept(this)).reduce((op1, op2) => (op1, op2) match {
          case (None, None) => None
          case (x, None) => x
          case (None, x) => x
          case (Some(var1), Some(var2)) if var1.equals(var2) =>
            Some(var1)
          case _ =>
            throw new ValidationException(s"Aggregation must be applied to a single pattern " +
              s"variable. Malformed expression: $call")
        })
      }
    }

    override def visitNode(rexNode: RexNode): Option[String] = None
  }

}
