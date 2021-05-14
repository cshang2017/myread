package org.apache.flink.table.planner.plan.rules.physical.stream

import org.apache.flink.table.planner.plan.nodes.FlinkConventions
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalCalc
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamExecCalc
import org.apache.flink.table.planner.plan.utils.PythonUtil.containsPythonCall
import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall, RelTraitSet}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.convert.ConverterRule

import scala.collection.JavaConverters._

/**
  * Rule that converts [[FlinkLogicalCalc]] to [[StreamExecCalc]].
  */
class StreamExecCalcRule
  extends ConverterRule(
    classOf[FlinkLogicalCalc],
    FlinkConventions.LOGICAL,
    FlinkConventions.STREAM_PHYSICAL,
    "StreamExecCalcRule") {

  override def matches(call: RelOptRuleCall): Boolean = {
    val calc: FlinkLogicalCalc = call.rel(0).asInstanceOf[FlinkLogicalCalc]
    val program = calc.getProgram
    !program.getExprList.asScala.exists(containsPythonCall(_))
  }

  def convert(rel: RelNode): RelNode = {
    val calc: FlinkLogicalCalc = rel.asInstanceOf[FlinkLogicalCalc]
    val traitSet: RelTraitSet = rel.getTraitSet.replace(FlinkConventions.STREAM_PHYSICAL)
    val newInput = RelOptRule.convert(calc.getInput, FlinkConventions.STREAM_PHYSICAL)

    new StreamExecCalc(
      rel.getCluster,
      traitSet,
      newInput,
      calc.getProgram,
      rel.getRowType)
  }
}

object StreamExecCalcRule {
  val INSTANCE: RelOptRule = new StreamExecCalcRule
}
