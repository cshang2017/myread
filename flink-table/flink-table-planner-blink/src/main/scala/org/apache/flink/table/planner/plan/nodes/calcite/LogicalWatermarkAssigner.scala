package org.apache.flink.table.planner.plan.nodes.calcite

import org.apache.calcite.plan._
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rex.RexNode

/**
  * Sub-class of [[WatermarkAssigner]] that is a relational operator
  * which generates [[org.apache.flink.streaming.api.watermark.Watermark]].
  * This class corresponds to Calcite logical rel.
  */
final class LogicalWatermarkAssigner(
    cluster: RelOptCluster,
    traits: RelTraitSet,
    input: RelNode,
    rowtimeFieldIndex: Int,
    watermarkExpr: RexNode)
  extends WatermarkAssigner(cluster, traits, input, rowtimeFieldIndex, watermarkExpr) {

  override def copy(
      traitSet: RelTraitSet,
      input: RelNode,
      rowtime: Int,
      watermark: RexNode): RelNode = {
    new LogicalWatermarkAssigner(cluster, traitSet, input, rowtime, watermark)
  }
}

object LogicalWatermarkAssigner {

  def create(
      cluster: RelOptCluster,
      input: RelNode,
      rowtimeFieldIndex: Int,
      watermarkExpr: RexNode): LogicalWatermarkAssigner = {
    val traits = cluster.traitSetOf(Convention.NONE)
    new LogicalWatermarkAssigner(cluster, traits, input, rowtimeFieldIndex, watermarkExpr)
  }
}

