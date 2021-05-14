package org.apache.flink.table.planner.expressions

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.table.planner.typeutils.TypeCoercion
import org.apache.flink.table.planner.validate._
import org.apache.flink.table.runtime.types.TypeInfoLogicalTypeConverter.fromTypeInfoToLogicalType

case class Cast(child: PlannerExpression, resultType: TypeInformation[_])
  extends UnaryExpression {

  override def toString = s"$child.cast($resultType)"

  override private[flink] def makeCopy(anyRefs: Array[AnyRef]): this.type = {
    val child: PlannerExpression = anyRefs.head.asInstanceOf[PlannerExpression]
    copy(child, resultType).asInstanceOf[this.type]
  }

  override private[flink] def validateInput(): ValidationResult = {
    if (TypeCoercion.canCast(
      fromTypeInfoToLogicalType(child.resultType),
      fromTypeInfoToLogicalType(resultType))) {
      ValidationSuccess
    } else {
      ValidationFailure(s"Unsupported cast from '${child.resultType}' to '$resultType'")
    }
  }
}
