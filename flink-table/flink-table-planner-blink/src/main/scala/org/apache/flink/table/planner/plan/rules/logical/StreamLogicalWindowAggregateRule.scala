package org.apache.flink.table.planner.plan.rules.logical

import org.apache.flink.table.api.{TableException, ValidationException}
import org.apache.flink.table.expressions.FieldReferenceExpression
import org.apache.flink.table.planner.calcite.FlinkTypeFactory
import org.apache.flink.table.planner.calcite.FlinkTypeFactory.toLogicalType
import org.apache.flink.table.runtime.types.LogicalTypeDataTypeConverter.fromLogicalTypeToDataType

import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.logical.{LogicalAggregate, LogicalProject}
import org.apache.calcite.rex._
import org.apache.calcite.sql.`type`.{SqlTypeFamily, SqlTypeName}

import _root_.java.math.{BigDecimal => JBigDecimal}

/**
  * Planner rule that transforms simple [[LogicalAggregate]] on a [[LogicalProject]]
  * with windowing expression to
 * [[org.apache.flink.table.planner.plan.nodes.calcite.LogicalWindowAggregate]]
 * for stream.
  */
class StreamLogicalWindowAggregateRule
  extends LogicalWindowAggregateRuleBase("StreamLogicalWindowAggregateRule") {

  /** Returns a reference to the time attribute with a time indicator type */
  override private[table] def getInAggregateGroupExpression(
      rexBuilder: RexBuilder,
      windowExpression: RexCall): RexNode = {

    val timeAttribute = windowExpression.operands.get(0)
    if (!FlinkTypeFactory.isTimeIndicatorType(timeAttribute.getType)) {
      throw new TableException(s"Window aggregate can only be defined over a " +
        s"time attribute column, but ${timeAttribute.getType} encountered.")
    }
    timeAttribute
  }

  /** Returns a zero literal of a timestamp type */
  override private[table] def getOutAggregateGroupExpression(
      rexBuilder: RexBuilder,
      windowExpression: RexCall): RexNode = {
    // Create a literal with normal SqlTypeName.TIMESTAMP
    // in case we reference a rowtime field.
    rexBuilder.makeLiteral(
      0L,
      rexBuilder.getTypeFactory.createSqlType(
        SqlTypeName.TIMESTAMP, windowExpression.getType.getPrecision),
      true)
  }

  private[table] override def getTimeFieldReference(
      operand: RexNode,
      timeAttributeIndex: Int,
      rowType: RelDataType): FieldReferenceExpression = {
    if (!FlinkTypeFactory.isTimeIndicatorType(operand.getType)) {
      throw new ValidationException("Window can only be defined over a time attribute column.")
    }

    val fieldName = rowType.getFieldList.get(timeAttributeIndex).getName
    val fieldType = rowType.getFieldList.get(timeAttributeIndex).getType
    new FieldReferenceExpression(
      fieldName,
      fromLogicalTypeToDataType(toLogicalType(fieldType)),
      0,
      timeAttributeIndex)
  }

  def getOperandAsLong(call: RexCall, idx: Int): Long =
    call.getOperands.get(idx) match {
      case v: RexLiteral if v.getTypeName.getFamily == SqlTypeFamily.INTERVAL_DAY_TIME =>
        v.getValue.asInstanceOf[JBigDecimal].longValue()
      case _: RexLiteral => throw new TableException(
        "Window aggregate only support SECOND, MINUTE, HOUR, DAY as the time unit. " +
          "MONTH and YEAR time unit are not supported yet.")
      case _ => throw new TableException("Only constant window descriptors are supported.")
    }
}

object StreamLogicalWindowAggregateRule {
  val INSTANCE = new StreamLogicalWindowAggregateRule
}
