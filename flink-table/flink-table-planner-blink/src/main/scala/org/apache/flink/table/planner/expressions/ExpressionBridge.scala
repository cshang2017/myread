
package org.apache.flink.table.planner.expressions

import org.apache.flink.table.expressions.{Expression, ExpressionVisitor}

/**
  * Bridges between API [[Expression]]s (for both Java and Scala) and final expression stack.
  */
class ExpressionBridge[E <: Expression](finalVisitor: ExpressionVisitor[E]) {

  def bridge(expression: Expression): E = {
    // convert to final expressions
    expression.accept(finalVisitor)
  }
}
