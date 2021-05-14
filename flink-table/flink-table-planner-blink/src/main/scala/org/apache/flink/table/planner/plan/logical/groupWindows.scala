

package org.apache.flink.table.planner.plan.logical

import org.apache.flink.table.expressions._
import org.apache.flink.table.planner.expressions.PlannerWindowReference

/**
  * Logical super class for group windows.
  *
  * @param aliasAttribute window alias
  * @param timeAttribute time field indicating event-time or processing-time
  */
abstract class LogicalWindow(
    val aliasAttribute: PlannerWindowReference,
    val timeAttribute: FieldReferenceExpression) {

  override def toString: String = getClass.getSimpleName
}

// ------------------------------------------------------------------------------------------------
// Tumbling group windows
// ------------------------------------------------------------------------------------------------

case class TumblingGroupWindow(
    alias: PlannerWindowReference,
    timeField: FieldReferenceExpression,
    size: ValueLiteralExpression)
  extends LogicalWindow(
    alias,
    timeField) {

  override def toString: String = s"TumblingGroupWindow($alias, $timeField, $size)"
}

// ------------------------------------------------------------------------------------------------
// Sliding group windows
// ------------------------------------------------------------------------------------------------

case class SlidingGroupWindow(
    alias: PlannerWindowReference,
    timeField: FieldReferenceExpression,
    size: ValueLiteralExpression,
    slide: ValueLiteralExpression)
  extends LogicalWindow(
    alias,
    timeField) {

  override def toString: String = s"SlidingGroupWindow($alias, $timeField, $size, $slide)"
}

// ------------------------------------------------------------------------------------------------
// Session group windows
// ------------------------------------------------------------------------------------------------

case class SessionGroupWindow(
    alias: PlannerWindowReference,
    timeField: FieldReferenceExpression,
    gap: ValueLiteralExpression)
  extends LogicalWindow(
    alias,
    timeField) {

  override def toString: String = s"SessionGroupWindow($alias, $timeField, $gap)"
}
