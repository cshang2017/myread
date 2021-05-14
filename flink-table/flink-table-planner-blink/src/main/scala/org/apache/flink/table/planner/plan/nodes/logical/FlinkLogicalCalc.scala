package org.apache.flink.table.planner.plan.nodes.logical

import org.apache.flink.table.planner.plan.nodes.FlinkConventions
import org.apache.flink.table.planner.plan.nodes.common.CommonCalc

import org.apache.calcite.plan._
import org.apache.calcite.rel.convert.ConverterRule
import org.apache.calcite.rel.core.Calc
import org.apache.calcite.rel.logical.LogicalCalc
import org.apache.calcite.rel.metadata.RelMdCollation
import org.apache.calcite.rel.{RelCollation, RelCollationTraitDef, RelNode}
import org.apache.calcite.rex.RexProgram

import java.util
import java.util.function.Supplier

/**
  * Sub-class of [[Calc]] that is a relational expression which computes project expressions
  * and also filters in Flink.
  */
class FlinkLogicalCalc(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    input: RelNode,
    calcProgram: RexProgram)
  extends CommonCalc(cluster, traitSet, input, calcProgram)
  with FlinkLogicalRel {

  override def copy(traitSet: RelTraitSet, child: RelNode, program: RexProgram): Calc = {
    new FlinkLogicalCalc(cluster, traitSet, child, program)
  }

}

private class FlinkLogicalCalcConverter
  extends ConverterRule(
    classOf[LogicalCalc],
    Convention.NONE,
    FlinkConventions.LOGICAL,
    "FlinkLogicalCalcConverter") {

  override def convert(rel: RelNode): RelNode = {
    val calc = rel.asInstanceOf[LogicalCalc]
    val newInput = RelOptRule.convert(calc.getInput, FlinkConventions.LOGICAL)
    FlinkLogicalCalc.create(newInput, calc.getProgram)
  }
}

object FlinkLogicalCalc {
  val CONVERTER: ConverterRule = new FlinkLogicalCalcConverter()

  def create(input: RelNode, calcProgram: RexProgram): FlinkLogicalCalc = {
    val cluster = input.getCluster
    val mq = cluster.getMetadataQuery
    val traitSet = cluster.traitSetOf(FlinkConventions.LOGICAL).replaceIfs(
      RelCollationTraitDef.INSTANCE, new Supplier[util.List[RelCollation]]() {
        def get: util.List[RelCollation] = RelMdCollation.calc(mq, input, calcProgram)
      }).simplify()
    new FlinkLogicalCalc(cluster, traitSet, input, calcProgram)
  }
}
