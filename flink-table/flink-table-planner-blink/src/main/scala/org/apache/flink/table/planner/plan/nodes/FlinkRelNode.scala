package org.apache.flink.table.planner.plan.nodes

import org.apache.flink.table.planner.plan.nodes.ExpressionFormat.ExpressionFormat
import org.apache.flink.table.planner.plan.utils.RelDescriptionWriterImpl

import org.apache.calcite.rel.RelNode
import org.apache.calcite.rex._
import org.apache.calcite.sql.SqlAsOperator
import org.apache.calcite.sql.SqlKind._

import java.io.{PrintWriter, StringWriter}

import scala.collection.JavaConversions._

/**
  * Base class for flink relational expression.
  */
trait FlinkRelNode extends RelNode {

  /**
    * Returns a string which describes the detailed information of relational expression
    * with attributes which contribute to the plan output.
    *
    * This method leverages [[RelNode#explain]] with
    * [[org.apache.calcite.sql.SqlExplainLevel.EXPPLAN_ATTRIBUTES]] explain level to generate
    * the description.
    */
  def getRelDetailedDescription: String = {
    val sw = new StringWriter
    val pw = new PrintWriter(sw)
    val relWriter = new RelDescriptionWriterImpl(pw)
    this.explain(relWriter)
    sw.toString
  }

  private[flink] def getExpressionString(
      expr: RexNode,
      inFields: List[String],
      localExprsTable: Option[List[RexNode]]): String = {
    getExpressionString(expr, inFields, localExprsTable, ExpressionFormat.Prefix)
  }

  private[flink] def getExpressionString(
      expr: RexNode,
      inFields: List[String],
      localExprsTable: Option[List[RexNode]],
      expressionFormat: ExpressionFormat): String = {

    expr match {
      case pr: RexPatternFieldRef =>
        val alpha = pr.getAlpha
        val field = inFields.get(pr.getIndex)
        s"$alpha.$field"

      case i: RexInputRef =>
        inFields.get(i.getIndex)

      case l: RexLiteral =>
        l.toString

      case l: RexLocalRef if localExprsTable.isEmpty =>
        throw new IllegalArgumentException("Encountered RexLocalRef without " +
          "local expression table")

      case l: RexLocalRef =>
        val lExpr = localExprsTable.get(l.getIndex)
        getExpressionString(lExpr, inFields, localExprsTable, expressionFormat)

      case c: RexCall =>
        val op = c.getOperator.toString
        val ops = c.getOperands.map(
          getExpressionString(_, inFields, localExprsTable, expressionFormat))
        c.getOperator match {
          case _ : SqlAsOperator => ops.head
          case _ =>
            expressionFormat match {
              case ExpressionFormat.Infix if ops.size() == 1 =>
                val operand = ops.head
                c.getKind match {
                  case IS_FALSE | IS_NOT_FALSE | IS_TRUE | IS_NOT_TRUE | IS_UNKNOWN | IS_NULL |
                       IS_NOT_NULL => s"$operand $op"
                  case _ => s"$op($operand)"
                }
              case ExpressionFormat.Infix => s"(${ops.mkString(s" $op ")})"
              case ExpressionFormat.PostFix => s"(${ops.mkString(", ")})$op"
              case ExpressionFormat.Prefix => s"$op(${ops.mkString(", ")})"
            }
        }

      case fa: RexFieldAccess =>
        val referenceExpr = getExpressionString(
          fa.getReferenceExpr,
          inFields,
          localExprsTable,
          expressionFormat)
        val field = fa.getField.getName
        s"$referenceExpr.$field"
      case cv: RexCorrelVariable =>
        cv.toString
      case _ =>
        throw new IllegalArgumentException(s"Unknown expression type '${expr.getClass}': $expr")
    }
  }
}

/**
  * Infix, Postfix and Prefix notations are three different but equivalent ways of writing
  * expressions. It is easiest to demonstrate the differences by looking at examples of operators
  * that take two operands.
  * Infix notation: (X + Y)
  * Postfix notation: (X Y) +
  * Prefix notation: + (X Y)
  */
object ExpressionFormat extends Enumeration {
  type ExpressionFormat = Value
  val Infix, PostFix, Prefix = Value
}

