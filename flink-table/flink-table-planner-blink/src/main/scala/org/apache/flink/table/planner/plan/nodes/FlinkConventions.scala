package org.apache.flink.table.planner.plan.nodes

import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalRel
import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchPhysicalRel
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalRel

import org.apache.calcite.plan.{Convention, RelTraitSet}
import org.apache.calcite.rel.RelNode

/**
  * Override the default convention implementation to support using AbstractConverter for conversion
  */
class FlinkConvention(name: String, relClass: Class[_ <: RelNode])
  extends Convention.Impl(name, relClass) {

  override def useAbstractConvertersForConversion(
      fromTraits: RelTraitSet,
      toTraits: RelTraitSet): Boolean = {
    if (relClass == classOf[StreamPhysicalRel]) {
      // stream
      !fromTraits.satisfies(toTraits) &&
        fromTraits.containsIfApplicable(FlinkConventions.STREAM_PHYSICAL) &&
        toTraits.containsIfApplicable(FlinkConventions.STREAM_PHYSICAL)
    } else {
      // batch
      !fromTraits.satisfies(toTraits) &&
        fromTraits.containsIfApplicable(FlinkConventions.BATCH_PHYSICAL) &&
        toTraits.containsIfApplicable(FlinkConventions.BATCH_PHYSICAL)
    }
  }
}

object FlinkConventions {
  val LOGICAL = new Convention.Impl("LOGICAL", classOf[FlinkLogicalRel])
  val STREAM_PHYSICAL = new FlinkConvention("STREAM_PHYSICAL", classOf[StreamPhysicalRel])
  val BATCH_PHYSICAL = new FlinkConvention("BATCH_PHYSICAL", classOf[BatchPhysicalRel])
}
