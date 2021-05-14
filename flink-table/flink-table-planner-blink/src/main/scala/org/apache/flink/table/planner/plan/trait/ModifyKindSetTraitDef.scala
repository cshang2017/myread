package org.apache.flink.table.planner.plan.`trait`

import org.apache.calcite.plan.{RelOptPlanner, RelTraitDef}
import org.apache.calcite.rel.RelNode

class ModifyKindSetTraitDef extends RelTraitDef[ModifyKindSetTrait] {

  override def getTraitClass: Class[ModifyKindSetTrait] = classOf[ModifyKindSetTrait]

  override def getSimpleName: String = this.getClass.getSimpleName

  override def convert(
      planner: RelOptPlanner,
      rel: RelNode,
      toTrait: ModifyKindSetTrait,
      allowInfiniteCostConverters: Boolean): RelNode = {
    rel.copy(rel.getTraitSet.plus(toTrait), rel.getInputs)
  }

  override def canConvert(
      planner: RelOptPlanner,
      fromTrait: ModifyKindSetTrait,
      toTrait: ModifyKindSetTrait): Boolean = {
    throw new UnsupportedOperationException(
      "ModifyKindSetTrait conversion is not supported for now.")
  }

  override def getDefault: ModifyKindSetTrait = ModifyKindSetTrait.EMPTY
}

object ModifyKindSetTraitDef {
  val INSTANCE = new ModifyKindSetTraitDef()
}


