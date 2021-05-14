package org.apache.flink.table.planner.plan.utils

import org.apache.calcite.rex._

/**
  * Implementation of [[RexVisitor]] that redirects all calls into generic
  * [[RexDefaultVisitor#visitNode(org.apache.calcite.rex.RexNode)]] method.
  */
abstract class RexDefaultVisitor[R] extends RexVisitor[R] {

  override def visitFieldAccess(fieldAccess: RexFieldAccess): R =
    visitNode(fieldAccess)

  override def visitCall(call: RexCall): R =
    visitNode(call)

  override def visitInputRef(inputRef: RexInputRef): R =
    visitNode(inputRef)

  override def visitOver(over: RexOver): R =
    visitNode(over)

  override def visitCorrelVariable(correlVariable: RexCorrelVariable): R =
    visitNode(correlVariable)

  override def visitLocalRef(localRef: RexLocalRef): R =
    visitNode(localRef)

  override def visitDynamicParam(dynamicParam: RexDynamicParam): R =
    visitNode(dynamicParam)

  override def visitRangeRef(rangeRef: RexRangeRef): R =
    visitNode(rangeRef)

  override def visitTableInputRef(tableRef: RexTableInputRef): R =
    visitNode(tableRef)

  override def visitPatternFieldRef(patternFieldRef: RexPatternFieldRef): R =
    visitNode(patternFieldRef)

  override def visitSubQuery(subQuery: RexSubQuery): R =
    visitNode(subQuery)

  override def visitLiteral(literal: RexLiteral): R =
    visitNode(literal)

  def visitNode(rexNode: RexNode): R
}
