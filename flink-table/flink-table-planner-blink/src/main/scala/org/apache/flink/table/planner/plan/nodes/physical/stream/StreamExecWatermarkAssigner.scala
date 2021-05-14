package org.apache.flink.table.planner.plan.nodes.physical.stream

import org.apache.flink.api.dag.Transformation
import org.apache.flink.streaming.api.transformations.OneInputTransformation
import org.apache.flink.table.api.config.ExecutionConfigOptions
import org.apache.flink.table.data.RowData
import org.apache.flink.table.planner.calcite.FlinkTypeFactory
import org.apache.flink.table.planner.codegen.WatermarkGeneratorCodeGenerator
import org.apache.flink.table.planner.delegation.StreamPlanner
import org.apache.flink.table.planner.plan.nodes.calcite.WatermarkAssigner
import org.apache.flink.table.planner.plan.nodes.exec.{ExecNode, StreamExecNode}
import org.apache.flink.table.planner.plan.utils.RelExplainUtil.preferExpressionFormat
import org.apache.flink.table.planner.utils.TableConfigUtils.getMillisecondFromConfigDuration
import org.apache.flink.table.runtime.operators.wmassigners.WatermarkAssignerOperatorFactory
import org.apache.flink.table.runtime.typeutils.RowDataTypeInfo

import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.{RelNode, RelWriter}
import org.apache.calcite.rex.RexNode

import java.util

import scala.collection.JavaConversions._

/**
  * Stream physical RelNode for [[WatermarkAssigner]].
  */
class StreamExecWatermarkAssigner(
    cluster: RelOptCluster,
    traits: RelTraitSet,
    inputRel: RelNode,
    rowtimeFieldIndex: Int,
    watermarkExpr: RexNode)
  extends WatermarkAssigner(cluster, traits, inputRel, rowtimeFieldIndex, watermarkExpr)
  with StreamPhysicalRel
  with StreamExecNode[RowData] {

  override def requireWatermark: Boolean = false

  override def copy(
      traitSet: RelTraitSet,
      input: RelNode,
      rowtime: Int,
      watermark: RexNode): RelNode = {
    new StreamExecWatermarkAssigner(cluster, traitSet, input, rowtime, watermark)
  }

  /**
    * Fully override this method to have a better display name of this RelNode.
    */
  override def explainTerms(pw: RelWriter): RelWriter = {
    val inFieldNames = inputRel.getRowType.getFieldNames.toList
    val rowtimeFieldName = inFieldNames(rowtimeFieldIndex)
    pw.input("input", getInput())
      .item("rowtime", rowtimeFieldName)
      .item("watermark", getExpressionString(
        watermarkExpr,
        inFieldNames,
        None,
        preferExpressionFormat(pw)))
  }

  //~ ExecNode methods -----------------------------------------------------------

  override def getInputNodes: util.List[ExecNode[StreamPlanner, _]] = {
    getInputs.map(_.asInstanceOf[ExecNode[StreamPlanner, _]])
  }

  override def replaceInputNode(
      ordinalInParent: Int,
      newInputNode: ExecNode[StreamPlanner, _]): Unit = {
    replaceInput(ordinalInParent, newInputNode.asInstanceOf[RelNode])
  }

  override protected def translateToPlanInternal(
      planner: StreamPlanner): Transformation[RowData] = {
    val inputTransformation = getInputNodes.get(0).translateToPlan(planner)
      .asInstanceOf[Transformation[RowData]]

    val config = planner.getTableConfig
    val idleTimeout = getMillisecondFromConfigDuration(config,
      ExecutionConfigOptions.TABLE_EXEC_SOURCE_IDLE_TIMEOUT)

    val watermarkGenerator = WatermarkGeneratorCodeGenerator.generateWatermarkGenerator(
      config,
      FlinkTypeFactory.toLogicalRowType(inputRel.getRowType),
      watermarkExpr)

    val operatorFactory = new WatermarkAssignerOperatorFactory(
        rowtimeFieldIndex,
        idleTimeout,
        watermarkGenerator)

    val outputRowTypeInfo = RowDataTypeInfo.of(FlinkTypeFactory.toLogicalRowType(getRowType))
    val transformation = new OneInputTransformation[RowData, RowData](
      inputTransformation,
      getRelDetailedDescription,
      operatorFactory,
      outputRowTypeInfo,
      inputTransformation.getParallelism)
    transformation
  }

}
