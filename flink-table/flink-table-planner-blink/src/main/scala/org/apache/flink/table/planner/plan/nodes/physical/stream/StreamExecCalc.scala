package org.apache.flink.table.planner.plan.nodes.physical.stream

import org.apache.flink.api.dag.Transformation
import org.apache.flink.streaming.api.transformations.OneInputTransformation
import org.apache.flink.table.data.RowData
import org.apache.flink.table.planner.calcite.FlinkTypeFactory
import org.apache.flink.table.planner.codegen.{CalcCodeGenerator, CodeGeneratorContext}
import org.apache.flink.table.planner.delegation.StreamPlanner
import org.apache.flink.table.runtime.operators.AbstractProcessStreamOperator
import org.apache.flink.table.runtime.typeutils.RowDataTypeInfo

import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.Calc
import org.apache.calcite.rex.RexProgram

/**
  * Stream physical RelNode for [[Calc]].
  */
class StreamExecCalc(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    inputRel: RelNode,
    calcProgram: RexProgram,
    outputRowType: RelDataType)
  extends StreamExecCalcBase(cluster, traitSet, inputRel, calcProgram, outputRowType) {

  override def copy(traitSet: RelTraitSet, child: RelNode, program: RexProgram): Calc = {
    new StreamExecCalc(cluster, traitSet, child, program, outputRowType)
  }

  override protected def translateToPlanInternal(
      planner: StreamPlanner): Transformation[RowData] = {
    val config = planner.getTableConfig
    val inputTransform = getInputNodes.get(0).translateToPlan(planner)
        .asInstanceOf[Transformation[RowData]]
    // materialize time attributes in condition
    val condition = if (calcProgram.getCondition != null) {
      Some(calcProgram.expandLocalRef(calcProgram.getCondition))
    } else {
      None
    }

    // TODO deal time indicators.
//    val condition = if (calcProgram.getCondition != null) {
//      val materializedCondition = RelTimeIndicatorConverter.convertExpression(
//        calcProgram.expandLocalRef(calcProgram.getCondition),
//        input.getRowType,
//        cluster.getRexBuilder)
//      Some(materializedCondition)
//    } else {
//      None
//    }

    val ctx = CodeGeneratorContext(config).setOperatorBaseClass(
      classOf[AbstractProcessStreamOperator[RowData]])
    val outputType = FlinkTypeFactory.toLogicalRowType(getRowType)
    val substituteStreamOperator = CalcCodeGenerator.generateCalcOperator(
      ctx,
      cluster,
      inputTransform,
      outputType,
      config,
      calcProgram,
      condition,
      retainHeader = true,
      "StreamExecCalc"
    )
    val ret = new OneInputTransformation(
      inputTransform,
      getRelDetailedDescription,
      substituteStreamOperator,
      RowDataTypeInfo.of(outputType),
      inputTransform.getParallelism)

    if (inputsContainSingleton()) {
      ret.setParallelism(1)
      ret.setMaxParallelism(1)
    }
    ret
  }
}
