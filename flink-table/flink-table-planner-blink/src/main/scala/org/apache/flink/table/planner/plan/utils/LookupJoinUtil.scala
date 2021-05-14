

package org.apache.flink.table.planner.plan.utils

import org.apache.flink.table.types.logical.LogicalType

import org.apache.calcite.rex.RexLiteral

/**
  * Utilities for temporal table join
  */
object LookupJoinUtil {

  /**
    * A [[LookupKey]] is a field used as equal condition when querying content from dimension table
    */
  sealed trait LookupKey

  /**
    * A [[LookupKey]] whose value is constant.
    * @param dataType the field type in TableSource
    * @param literal the literal value
    */
  case class ConstantLookupKey(dataType: LogicalType, literal: RexLiteral) extends LookupKey

  /**
    * A [[LookupKey]] whose value comes from left table field.
    * @param index the index of the field in left table
    */
  case class FieldRefLookupKey(index: Int) extends LookupKey

}
