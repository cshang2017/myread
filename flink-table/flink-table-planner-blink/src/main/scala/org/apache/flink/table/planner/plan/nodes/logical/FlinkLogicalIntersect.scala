package org.apache.flink.table.planner.plan.nodes.logical

import org.apache.flink.table.planner.plan.nodes.FlinkConventions

import org.apache.calcite.plan._
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.convert.ConverterRule
import org.apache.calcite.rel.core.{Intersect, SetOp}
import org.apache.calcite.rel.logical.LogicalIntersect
import org.apache.calcite.rel.metadata.RelMetadataQuery

import java.util

import scala.collection.JavaConversions._

/**
  * Sub-class of [[Intersect]] that is a relational expression
  * which returns the intersection of the rows of its inputs in Flink.
  */
class FlinkLogicalIntersect(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    inputs: util.List[RelNode],
    all: Boolean)
  extends Intersect(cluster, traitSet, inputs, all)
  with FlinkLogicalRel {

  override def copy(traitSet: RelTraitSet, inputs: util.List[RelNode], all: Boolean): SetOp = {
    new FlinkLogicalIntersect(cluster, traitSet, inputs, all)
  }

  override def computeSelfCost(planner: RelOptPlanner, mq: RelMetadataQuery): RelOptCost = {
    val zeroCost = planner.getCostFactory.makeCost(0, 0, 0)
    this.getInputs.foldLeft(zeroCost) {
      (cost, input) =>
        val rowCnt = mq.getRowCount(input)
        val rowSize = mq.getAverageRowSize(input)
        val inputCost = planner.getCostFactory.makeCost(rowCnt, rowCnt, rowCnt * rowSize)
        cost.plus(inputCost)
    }
  }

}

private class FlinkLogicalIntersectConverter
  extends ConverterRule(
    classOf[LogicalIntersect],
    Convention.NONE,
    FlinkConventions.LOGICAL,
    "FlinkLogicalIntersectConverter") {

  override def convert(rel: RelNode): RelNode = {
    val intersect = rel.asInstanceOf[LogicalIntersect]
    val newInputs = intersect.getInputs.map {
      input => RelOptRule.convert(input, FlinkConventions.LOGICAL)
    }
    FlinkLogicalIntersect.create(newInputs, intersect.all)
  }
}

object FlinkLogicalIntersect {
  val CONVERTER: ConverterRule = new FlinkLogicalIntersectConverter()

  def create(
      inputs: util.List[RelNode],
      all: Boolean): FlinkLogicalIntersect = {
    val cluster = inputs.get(0).getCluster
    val traitSet = cluster.traitSetOf(FlinkConventions.LOGICAL).simplify()
    new FlinkLogicalIntersect(cluster, traitSet, inputs, all)
  }
}
