
package org.apache.flink.table.planner.plan.`trait`

import org.apache.calcite.sql.validate.SqlMonotonicity

/**
  * The RelModifiedMonotonicity is used to describe the modified monotonicity of a
  * relation expression. Every field has its own modified monotonicity which means
  * the direction of the field's value is updated.
  */
class RelModifiedMonotonicity(val fieldMonotonicities: Array[SqlMonotonicity]) {

  override def equals(obj: scala.Any): Boolean = {

    if (obj == null || getClass != obj.getClass) {
      return false
    }

    val o = obj.asInstanceOf[RelModifiedMonotonicity]
    fieldMonotonicities.deep == o.fieldMonotonicities.deep
  }

  override def toString: String = {
    s"[${fieldMonotonicities.mkString(",")}]"
  }
}
