

package org.apache.flink.table.planner.plan.nodes.logical

import org.apache.flink.table.planner.plan.nodes.FlinkConventions

import org.apache.calcite.plan.{Convention, RelOptCluster, RelOptRule, RelTraitSet}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.convert.ConverterRule
import org.apache.calcite.rel.core.{Correlate, CorrelationId, JoinRelType}
import org.apache.calcite.rel.logical.LogicalCorrelate
import org.apache.calcite.util.ImmutableBitSet

/**
  * Sub-class of [[Correlate]] that is a relational operator
  * which performs nested-loop joins in Flink.
  */
class FlinkLogicalCorrelate(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    left: RelNode,
    right: RelNode,
    correlationId: CorrelationId,
    requiredColumns: ImmutableBitSet,
    joinType: JoinRelType)
  extends Correlate(cluster, traitSet, left, right, correlationId, requiredColumns, joinType)
  with FlinkLogicalRel {

  override def copy(
      traitSet: RelTraitSet,
      left: RelNode,
      right: RelNode,
      correlationId: CorrelationId,
      requiredColumns: ImmutableBitSet,
      joinType: JoinRelType): Correlate = {

    new FlinkLogicalCorrelate(
      cluster,
      traitSet,
      left,
      right,
      correlationId,
      requiredColumns,
      joinType)
  }

}

class FlinkLogicalCorrelateConverter
  extends ConverterRule(
    classOf[LogicalCorrelate],
    Convention.NONE,
    FlinkConventions.LOGICAL,
    "FlinkLogicalCorrelateConverter") {

  override def convert(rel: RelNode): RelNode = {
    val correlate = rel.asInstanceOf[LogicalCorrelate]
    val newLeft = RelOptRule.convert(correlate.getLeft, FlinkConventions.LOGICAL)
    val newRight = RelOptRule.convert(correlate.getRight, FlinkConventions.LOGICAL)
    FlinkLogicalCorrelate.create(
      newLeft,
      newRight,
      correlate.getCorrelationId,
      correlate.getRequiredColumns,
      correlate.getJoinType)
  }
}

object FlinkLogicalCorrelate {
  val CONVERTER: ConverterRule = new FlinkLogicalCorrelateConverter()

  def create(
      left: RelNode,
      right: RelNode,
      correlationId: CorrelationId,
      requiredColumns: ImmutableBitSet,
      joinType: JoinRelType): FlinkLogicalCorrelate = {
    val cluster = left.getCluster
    val traitSet = cluster.traitSetOf(FlinkConventions.LOGICAL).simplify()
    new FlinkLogicalCorrelate(
      cluster, traitSet, left, right, correlationId, requiredColumns, joinType)
  }
}
