
package org.apache.flink.table.planner.expressions

import org.apache.flink.api.common.typeinfo.BasicTypeInfo._
import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation}
import org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable
import org.apache.flink.table.planner.typeutils.TypeInfoCheckUtils.{isArray, isComparable, isNumeric}
import org.apache.flink.table.planner.validate._
import org.apache.flink.table.runtime.types.TypeInfoLogicalTypeConverter.fromTypeInfoToLogicalType

import org.apache.calcite.sql.SqlOperator

abstract class BinaryComparison extends BinaryExpression {
  private[flink] def sqlOperator: SqlOperator

  override private[flink] def resultType = BOOLEAN_TYPE_INFO

  override private[flink] def validateInput(): ValidationResult =
    (left.resultType, right.resultType) match {
      case (lType, rType) if isNumeric(lType) && isNumeric(rType) => ValidationSuccess
      case (lType, rType) if isComparable(lType) && lType == rType => ValidationSuccess
      case (lType, rType) if isComparable(lType) &&
          fromTypeInfoToLogicalType(lType) == fromTypeInfoToLogicalType(rType) =>
        ValidationSuccess
      case (lType, rType) =>
        ValidationFailure(
          s"Comparison is only supported for numeric types and " +
            s"comparable types of same type, got $lType and $rType")
    }
}

case class EqualTo(left: PlannerExpression, right: PlannerExpression) extends BinaryComparison {
  override def toString = s"$left === $right"

  private[flink] val sqlOperator: SqlOperator = FlinkSqlOperatorTable.EQUALS

  override private[flink] def validateInput(): ValidationResult =
    (left.resultType, right.resultType) match {
      case (lType, rType) if isNumeric(lType) && isNumeric(rType) => ValidationSuccess
      case (lType, rType) if lType == rType => ValidationSuccess
      case (lType, rType)
        if fromTypeInfoToLogicalType(lType) == fromTypeInfoToLogicalType(rType) =>
        ValidationSuccess
      case (lType, rType) if isArray(lType) && lType.getTypeClass == rType.getTypeClass =>
        ValidationSuccess
      case (lType, rType) =>
        ValidationFailure(s"Equality predicate on incompatible types: $lType and $rType")
    }
}

case class NotEqualTo(left: PlannerExpression, right: PlannerExpression) extends BinaryComparison {
  override def toString = s"$left !== $right"

  private[flink] val sqlOperator: SqlOperator = FlinkSqlOperatorTable.NOT_EQUALS

  override private[flink] def validateInput(): ValidationResult =
    (left.resultType, right.resultType) match {
      case (lType, rType) if isNumeric(lType) && isNumeric(rType) => ValidationSuccess
      case (lType, rType) if lType == rType => ValidationSuccess
      case (lType, rType)
        if fromTypeInfoToLogicalType(lType) == fromTypeInfoToLogicalType(rType) =>
        ValidationSuccess
      case (lType, rType) if isArray(lType) && lType.getTypeClass == rType.getTypeClass =>
        ValidationSuccess
      case (lType, rType) =>
        ValidationFailure(s"Inequality predicate on incompatible types: $lType and $rType")
    }
}

case class GreaterThan(left: PlannerExpression, right: PlannerExpression) extends BinaryComparison {
  override def toString = s"$left > $right"

  private[flink] val sqlOperator: SqlOperator = FlinkSqlOperatorTable.GREATER_THAN
}

case class GreaterThanOrEqual(left: PlannerExpression, right: PlannerExpression)
  extends BinaryComparison {
  override def toString = s"$left >= $right"

  private[flink] val sqlOperator: SqlOperator = FlinkSqlOperatorTable.GREATER_THAN_OR_EQUAL
}

case class LessThan(left: PlannerExpression, right: PlannerExpression) extends BinaryComparison {
  override def toString = s"$left < $right"

  private[flink] val sqlOperator: SqlOperator = FlinkSqlOperatorTable.LESS_THAN
}

case class LessThanOrEqual(left: PlannerExpression, right: PlannerExpression)
  extends BinaryComparison {
  override def toString = s"$left <= $right"

  private[flink] val sqlOperator: SqlOperator = FlinkSqlOperatorTable.LESS_THAN_OR_EQUAL
}

case class IsNull(child: PlannerExpression) extends UnaryExpression {
  override def toString = s"($child).isNull"

  override private[flink] def resultType = BOOLEAN_TYPE_INFO
}

case class IsNotNull(child: PlannerExpression) extends UnaryExpression {
  override def toString = s"($child).isNotNull"

  override private[flink] def resultType = BOOLEAN_TYPE_INFO
}

case class IsTrue(child: PlannerExpression) extends UnaryExpression {
  override def toString = s"($child).isTrue"

  override private[flink] def resultType = BOOLEAN_TYPE_INFO
}

case class IsFalse(child: PlannerExpression) extends UnaryExpression {
  override def toString = s"($child).isFalse"

  override private[flink] def resultType = BOOLEAN_TYPE_INFO
}

case class IsNotTrue(child: PlannerExpression) extends UnaryExpression {
  override def toString = s"($child).isNotTrue"

  override private[flink] def resultType = BOOLEAN_TYPE_INFO
}

case class IsNotFalse(child: PlannerExpression) extends UnaryExpression {
  override def toString = s"($child).isNotFalse"

  override private[flink] def resultType = BOOLEAN_TYPE_INFO
}

abstract class BetweenComparison(
    expr: PlannerExpression,
    lowerBound: PlannerExpression,
    upperBound: PlannerExpression)
  extends PlannerExpression {

  override private[flink] def resultType: TypeInformation[_] = BasicTypeInfo.BOOLEAN_TYPE_INFO

  override private[flink] def children: Seq[PlannerExpression] = Seq(expr, lowerBound, upperBound)

  override private[flink] def validateInput(): ValidationResult = {
    (expr.resultType, lowerBound.resultType, upperBound.resultType) match {
      case (exprType, lowerType, upperType)
          if isNumeric(exprType) && isNumeric(lowerType) && isNumeric(upperType) =>
        ValidationSuccess
      case (exprType, lowerType, upperType)
          if isComparable(exprType) && exprType == lowerType && exprType == upperType =>
        ValidationSuccess
      case (exprType, lowerType, upperType) =>
        ValidationFailure(
          s"Between is only supported for numeric types and " +
            s"identical comparable types, but got $exprType, $lowerType and $upperType"
        )
    }
  }
}

case class Between(
    expr: PlannerExpression,
    lowerBound: PlannerExpression,
    upperBound: PlannerExpression)
  extends BetweenComparison(expr, lowerBound, upperBound) {

  override def toString: String = s"($expr).between($lowerBound, $upperBound)"
}

case class NotBetween(
    expr: PlannerExpression,
    lowerBound: PlannerExpression,
    upperBound: PlannerExpression)
  extends BetweenComparison(expr, lowerBound, upperBound) {

  override def toString: String = s"($expr).notBetween($lowerBound, $upperBound)"
}
