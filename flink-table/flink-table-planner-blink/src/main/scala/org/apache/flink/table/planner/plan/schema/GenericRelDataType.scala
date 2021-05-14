package org.apache.flink.table.planner.plan.schema

import org.apache.calcite.rel.`type`.RelDataTypeSystem
import org.apache.calcite.sql.`type`.{ArraySqlType, BasicSqlType, MapSqlType, SqlTypeName}
import java.lang

import org.apache.flink.table.types.logical.TypeInformationRawType

/**
  * Generic type for encapsulating Flink's [[TypeInformationRawType]].
  *
  * @param genericType LogicalType to encapsulate
  * @param nullable flag if type can be nullable
  * @param typeSystem Flink's type system
  */
class GenericRelDataType(
    val genericType: TypeInformationRawType[_],
    val nullable: Boolean,
    typeSystem: RelDataTypeSystem)
  extends BasicSqlType(
    typeSystem,
    SqlTypeName.ANY) {

  isNullable = nullable
  computeDigest()

  override def toString = s"RAW($genericType)"

  def canEqual(other: Any): Boolean = other.isInstanceOf[GenericRelDataType]

  override def equals(other: Any): Boolean = other match {
    case that: GenericRelDataType =>
      super.equals(that) &&
        (that canEqual this) &&
        genericType == that.genericType &&
        nullable == that.nullable
    case _ => false
  }

  override def hashCode(): Int = {
    genericType.hashCode()
  }

  /**
    * [[ArraySqlType]], [[MapSqlType]]... use generateTypeString to equals and hashcode.
    */
  override def generateTypeString(sb: lang.StringBuilder, withDetail: Boolean): Unit = {
    sb.append(s"RAW('${genericType.getTypeInformation}')")
  }
}
