
package org.apache.flink.table.planner.plan.`trait`

import org.apache.calcite.plan.{RelOptPlanner, RelTraitDef}
import org.apache.calcite.rel.RelNode

class FlinkRelDistributionTraitDef extends RelTraitDef[FlinkRelDistribution] {

  override def getDefault: FlinkRelDistribution = FlinkRelDistribution.DEFAULT

  override def getTraitClass: Class[FlinkRelDistribution] = classOf[FlinkRelDistribution]

  override def getSimpleName: String = this.getClass.getSimpleName

  override def canConvert(
      planner: RelOptPlanner,
      fromTrait: FlinkRelDistribution,
      toTrait: FlinkRelDistribution): Boolean = true

  override def convert(
      planner: RelOptPlanner,
      rel: RelNode,
      toTrait: FlinkRelDistribution,
      allowInfiniteCostConverters: Boolean): RelNode = {
    throw new RuntimeException("Don't invoke FlinkRelDistributionTraitDef.convert directly!")
  }

}

object FlinkRelDistributionTraitDef {
  val INSTANCE = new FlinkRelDistributionTraitDef
}
