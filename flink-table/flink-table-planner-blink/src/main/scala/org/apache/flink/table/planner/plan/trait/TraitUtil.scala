

package org.apache.flink.table.planner.plan.`trait`

import org.apache.calcite.plan.RelTrait
import org.apache.calcite.rel.{RelCollation, RelCollations, RelFieldCollation}
import org.apache.calcite.util.mapping.Mappings

import scala.collection.JavaConversions._
import scala.collection.mutable

/**
  * Utility for [[RelTrait]]
  */
object TraitUtil {

  /**
    * Apply collation based on the given mapping restrict. Returns RelCollations.EMPTY if there
    * exists collation fields which has no target values in the given mapping.
    *
    * @param collation collation which to apply mapping
    * @param mapping mapping columns to a target.
    * @return A new collation after apply collation based on the given mapping restrict.
    *         Returns RelCollations.EMPTY if there exists collation fields which has no target
    *         values in the given mapping.
    */
  def apply(collation: RelCollation, mapping: Mappings.TargetMapping): RelCollation = {
    val fieldCollations = collation.getFieldCollations
    if (fieldCollations.isEmpty) collation
    else {
      val newFieldCollations = mutable.ArrayBuffer[RelFieldCollation]()
      fieldCollations.foreach { fieldCollation =>
        try {
          val i = mapping.getTargetOpt(fieldCollation.getFieldIndex)
          if (i >= 0) newFieldCollations.add(fieldCollation.copy(i)) else return RelCollations.EMPTY
        } catch {
          case _: IndexOutOfBoundsException => return RelCollations.EMPTY
        }
      }
      RelCollations.of(newFieldCollations: _*)
    }
  }

}
