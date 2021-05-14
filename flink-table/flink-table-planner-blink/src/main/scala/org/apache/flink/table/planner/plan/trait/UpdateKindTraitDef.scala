

package org.apache.flink.table.planner.plan.`trait`

import org.apache.calcite.plan.{RelOptPlanner, RelTraitDef}
import org.apache.calcite.rel.RelNode

class UpdateKindTraitDef extends RelTraitDef[UpdateKindTrait] {

  override def getTraitClass: Class[UpdateKindTrait] = classOf[UpdateKindTrait]

  override def getSimpleName: String = this.getClass.getSimpleName

  override def convert(
      planner: RelOptPlanner,
      rel: RelNode,
      toTrait: UpdateKindTrait,
      allowInfiniteCostConverters: Boolean): RelNode = {
    rel.copy(rel.getTraitSet.plus(toTrait), rel.getInputs)
  }

  override def canConvert(
      planner: RelOptPlanner,
      fromTrait: UpdateKindTrait,
      toTrait: UpdateKindTrait): Boolean = {
    throw new UnsupportedOperationException(
      "UpdateKindTrait conversion is not supported for now.")
  }

  override def getDefault: UpdateKindTrait = UpdateKindTrait.NONE
}

object UpdateKindTraitDef {
  val INSTANCE = new UpdateKindTraitDef()
}

