
package org.apache.flink.table.planner.plan.utils

import org.apache.flink.util.Preconditions.checkArgument

import org.apache.calcite.rex._
import org.apache.calcite.sql.`type`.{OperandTypes, ReturnTypes}
import org.apache.calcite.sql.{SqlFunction, SqlFunctionCategory, SqlKind}

/**
  * Utilities for temporal table join
  */
object TemporalJoinUtil {

  // ----------------------------------------------------------------------------------------
  //                          Temporal TableFunction Join Utilities
  // ----------------------------------------------------------------------------------------

  /**
    * [[TEMPORAL_JOIN_CONDITION]] is a specific condition which correctly defines
    * references to rightTimeAttribute, rightPrimaryKeyExpression and leftTimeAttribute.
    * The condition is used to mark this is a temporal tablefunction join.
    * Later rightTimeAttribute, rightPrimaryKeyExpression and leftTimeAttribute will be
    * extracted from the condition.
    */
  val TEMPORAL_JOIN_CONDITION = new SqlFunction(
    "__TEMPORAL_JOIN_CONDITION",
    SqlKind.OTHER_FUNCTION,
    ReturnTypes.BOOLEAN_NOT_NULL,
    null,
    OperandTypes.or(
      OperandTypes.sequence(
        "'(LEFT_TIME_ATTRIBUTE, RIGHT_TIME_ATTRIBUTE, PRIMARY_KEY)'",
        OperandTypes.DATETIME,
        OperandTypes.DATETIME,
        OperandTypes.ANY),
      OperandTypes.sequence(
        "'(LEFT_TIME_ATTRIBUTE, PRIMARY_KEY)'",
        OperandTypes.DATETIME,
        OperandTypes.ANY)),
    SqlFunctionCategory.SYSTEM)

  def isRowtimeCall(call: RexCall): Boolean = {
    checkArgument(call.getOperator == TEMPORAL_JOIN_CONDITION)
    call.getOperands.size() == 3
  }

  def isProctimeCall(call: RexCall): Boolean = {
    checkArgument(call.getOperator == TEMPORAL_JOIN_CONDITION)
    call.getOperands.size() == 2
  }

  def makeRowTimeTemporalJoinConditionCall(
    rexBuilder: RexBuilder,
    leftTimeAttribute: RexNode,
    rightTimeAttribute: RexNode,
    rightPrimaryKeyExpression: RexNode): RexNode = {
    rexBuilder.makeCall(
      TEMPORAL_JOIN_CONDITION,
      leftTimeAttribute,
      rightTimeAttribute,
      rightPrimaryKeyExpression)
  }

  def makeProcTimeTemporalJoinConditionCall(
    rexBuilder: RexBuilder,
    leftTimeAttribute: RexNode,
    rightPrimaryKeyExpression: RexNode): RexNode = {
    rexBuilder.makeCall(
      TEMPORAL_JOIN_CONDITION,
      leftTimeAttribute,
      rightPrimaryKeyExpression)
  }

  def containsTemporalJoinCondition(condition: RexNode): Boolean = {
    var hasTemporalJoinCondition: Boolean = false
    condition.accept(new RexVisitorImpl[Void](true) {
      override def visitCall(call: RexCall): Void = {
        if (call.getOperator != TEMPORAL_JOIN_CONDITION) {
          super.visitCall(call)
        } else {
          hasTemporalJoinCondition = true
          null
        }
      }
    })
    hasTemporalJoinCondition
  }

}
