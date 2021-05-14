

package org.apache.flink.table.planner.plan.schema

import org.apache.calcite.rel.`type`.RelDataTypeSystem
import org.apache.calcite.sql.`type`.BasicSqlType

import java.lang

/**
  * Creates a time indicator type for event-time or processing-time, but with similar properties
  * as a basic SQL type.
  */
class TimeIndicatorRelDataType(
    val typeSystem: RelDataTypeSystem,
    val originalType: BasicSqlType,
    val nullable: Boolean,
    val isEventTime: Boolean)
  extends BasicSqlType(
    typeSystem,
    originalType.getSqlTypeName,
    originalType.getPrecision) {

  this.isNullable = nullable
  computeDigest()

  override def hashCode(): Int = {
    super.hashCode() + 42 // we change the hash code to differentiate from regular timestamps
  }

  override def toString: String = {
    s"TIME ATTRIBUTE(${if (isEventTime) "ROWTIME" else "PROCTIME"})"
  }

  override def generateTypeString(sb: lang.StringBuilder, withDetail: Boolean): Unit = {
    sb.append(toString)
  }
}
