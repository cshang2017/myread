package org.apache.flink.table.planner.plan.nodes.physical.stream

import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.dag.Transformation
import org.apache.flink.runtime.state.KeyGroupRangeAssignment.DEFAULT_LOWER_BOUND_MAX_PARALLELISM
import org.apache.flink.streaming.api.transformations.PartitionTransformation
import org.apache.flink.streaming.runtime.partitioner.{GlobalPartitioner, KeyGroupStreamPartitioner, StreamPartitioner}
import org.apache.flink.table.data.RowData
import org.apache.flink.table.planner.calcite.FlinkTypeFactory
import org.apache.flink.table.planner.delegation.StreamPlanner
import org.apache.flink.table.planner.plan.nodes.common.CommonPhysicalExchange
import org.apache.flink.table.planner.plan.nodes.exec.{ExecNode, StreamExecNode}
import org.apache.flink.table.planner.plan.utils.KeySelectorUtil
import org.apache.flink.table.runtime.typeutils.RowDataTypeInfo

import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.{RelDistribution, RelNode}

import java.util

import scala.collection.JavaConversions._

/**
  * Stream physical RelNode for [[org.apache.calcite.rel.core.Exchange]].
  */
class StreamExecExchange(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    relNode: RelNode,
    relDistribution: RelDistribution)
  extends CommonPhysicalExchange(cluster, traitSet, relNode, relDistribution)
  with StreamPhysicalRel
  with StreamExecNode[RowData] {

  override def requireWatermark: Boolean = false

  override def copy(
      traitSet: RelTraitSet,
      newInput: RelNode,
      newDistribution: RelDistribution): StreamExecExchange = {
    new StreamExecExchange(cluster, traitSet, newInput, newDistribution)
  }

  //~ ExecNode methods -----------------------------------------------------------

  override def getInputNodes: util.List[ExecNode[StreamPlanner, _]] = {
    List(getInput.asInstanceOf[ExecNode[StreamPlanner, _]])
  }

  override def replaceInputNode(
      ordinalInParent: Int,
      newInputNode: ExecNode[StreamPlanner, _]): Unit = {
    replaceInput(ordinalInParent, newInputNode.asInstanceOf[RelNode])
  }

  override protected def translateToPlanInternal(
      planner: StreamPlanner): Transformation[RowData] = {
    val inputTransform = getInputNodes.get(0).translateToPlan(planner)
      .asInstanceOf[Transformation[RowData]]
    val inputTypeInfo = inputTransform.getOutputType.asInstanceOf[RowDataTypeInfo]
    val outputTypeInfo = RowDataTypeInfo.of(
      FlinkTypeFactory.toLogicalRowType(getRowType))
    relDistribution.getType match {
      case RelDistribution.Type.SINGLETON =>
        val partitioner = new GlobalPartitioner[RowData]
        val transformation = new PartitionTransformation(
          inputTransform,
          partitioner.asInstanceOf[StreamPartitioner[RowData]])
        transformation.setOutputType(outputTypeInfo)
        transformation.setParallelism(1)
        transformation
      case RelDistribution.Type.HASH_DISTRIBUTED =>
        // TODO Eliminate duplicate keys

        val selector = KeySelectorUtil.getRowDataSelector(
          relDistribution.getKeys.map(_.toInt).toArray, inputTypeInfo)
        val partitioner = new KeyGroupStreamPartitioner(selector,
          DEFAULT_LOWER_BOUND_MAX_PARALLELISM)
        val transformation = new PartitionTransformation(
          inputTransform,
          partitioner.asInstanceOf[StreamPartitioner[RowData]])
        transformation.setOutputType(outputTypeInfo)
        transformation.setParallelism(ExecutionConfig.PARALLELISM_DEFAULT)
        transformation
      case _ =>
        throw new UnsupportedOperationException(
          s"not support RelDistribution: ${relDistribution.getType} now!")
    }
  }

}
