package org.apache.flink.table.plan.rules.datastream

import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall, RelTraitSet}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.convert.ConverterRule
import org.apache.flink.table.plan.nodes.FlinkConventions
import org.apache.flink.table.plan.nodes.datastream.DataStreamCalc
import org.apache.flink.table.plan.nodes.logical.FlinkLogicalCalc
import org.apache.flink.table.plan.schema.RowSchema
import org.apache.flink.table.plan.util.PythonUtil.containsPythonCall

import scala.collection.JavaConverters._

class DataStreamCalcRule
  extends ConverterRule(
    classOf[FlinkLogicalCalc],
    FlinkConventions.LOGICAL,
    FlinkConventions.DATASTREAM,
    "DataStreamCalcRule") {

  override def matches(call: RelOptRuleCall): Boolean = {
    val calc: FlinkLogicalCalc = call.rel(0).asInstanceOf[FlinkLogicalCalc]
    val program = calc.getProgram
    !program.getExprList.asScala.exists(containsPythonCall(_))
  }

  def convert(rel: RelNode): RelNode = {
    val calc: FlinkLogicalCalc = rel.asInstanceOf[FlinkLogicalCalc]
    val traitSet: RelTraitSet = rel.getTraitSet.replace(FlinkConventions.DATASTREAM)
    val convInput: RelNode = RelOptRule.convert(calc.getInput, FlinkConventions.DATASTREAM)

    new DataStreamCalc(
      rel.getCluster,
      traitSet,
      convInput,
      new RowSchema(convInput.getRowType),
      new RowSchema(rel.getRowType),
      calc.getProgram,
      "DataStreamCalcRule")
  }
}

object DataStreamCalcRule {
  val INSTANCE: RelOptRule = new DataStreamCalcRule
}
