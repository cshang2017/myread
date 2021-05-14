package org.apache.flink.table.planner.plan.rules.physical.stream

import org.apache.calcite.plan.{RelOptRule, RelTraitSet}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.convert.ConverterRule
import org.apache.flink.table.planner.plan.nodes.FlinkConventions
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalWatermarkAssigner
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamExecWatermarkAssigner

/**
  * Rule that converts [[FlinkLogicalWatermarkAssigner]] to [[StreamExecWatermarkAssigner]].
  */
class StreamExecWatermarkAssignerRule
  extends ConverterRule(
    classOf[FlinkLogicalWatermarkAssigner],
    FlinkConventions.LOGICAL,
    FlinkConventions.STREAM_PHYSICAL,
    "StreamExecWatermarkAssignerRule") {

  override def convert(rel: RelNode): RelNode = {
    val watermarkAssigner = rel.asInstanceOf[FlinkLogicalWatermarkAssigner]
    val convertInput = RelOptRule.convert(
      watermarkAssigner.getInput, FlinkConventions.STREAM_PHYSICAL)
    val traitSet: RelTraitSet = rel.getTraitSet.replace(FlinkConventions.STREAM_PHYSICAL)

    new StreamExecWatermarkAssigner(
      watermarkAssigner.getCluster,
      traitSet,
      convertInput,
      watermarkAssigner.rowtimeFieldIndex,
      watermarkAssigner.watermarkExpr
    )
  }
}

object StreamExecWatermarkAssignerRule {
  val INSTANCE = new StreamExecWatermarkAssignerRule
}
