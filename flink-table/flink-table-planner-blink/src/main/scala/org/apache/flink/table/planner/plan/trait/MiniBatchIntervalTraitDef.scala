
package org.apache.flink.table.planner.plan.`trait`

import org.apache.calcite.plan.{RelOptPlanner, RelTraitDef}
import org.apache.calcite.rel.RelNode

class MiniBatchIntervalTraitDef extends RelTraitDef[MiniBatchIntervalTrait] {

  override def getTraitClass: Class[MiniBatchIntervalTrait] = classOf[MiniBatchIntervalTrait]

  override def getSimpleName: String = this.getClass.getSimpleName

  override def convert(
      planner: RelOptPlanner,
      rel: RelNode,
      toTrait: MiniBatchIntervalTrait,
      allowInfiniteCostConverters: Boolean): RelNode = {
    rel.copy(rel.getTraitSet.plus(toTrait), rel.getInputs)
  }

  override def canConvert(
      planner: RelOptPlanner,
      fromTrait: MiniBatchIntervalTrait,
      toTrait: MiniBatchIntervalTrait): Boolean = true

  override def getDefault: MiniBatchIntervalTrait = MiniBatchIntervalTrait.NONE
}

object MiniBatchIntervalTraitDef {
  val INSTANCE = new MiniBatchIntervalTraitDef
}
