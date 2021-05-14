package org.apache.flink.table.planner.plan.nodes.physical.stream

import java.util

import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.Calc
import org.apache.calcite.rex.RexProgram
import org.apache.flink.table.data.RowData
import org.apache.flink.table.planner.delegation.StreamPlanner
import org.apache.flink.table.planner.plan.nodes.common.CommonCalc
import org.apache.flink.table.planner.plan.nodes.exec.{ExecNode, StreamExecNode}

import scala.collection.JavaConversions._

/**
  * Base stream physical RelNode for [[Calc]].
  */
abstract class StreamExecCalcBase(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    inputRel: RelNode,
    calcProgram: RexProgram,
    outputRowType: RelDataType)
  extends CommonCalc(cluster, traitSet, inputRel, calcProgram)
  with StreamPhysicalRel
  with StreamExecNode[RowData] {

  override def requireWatermark: Boolean = false

  override def deriveRowType(): RelDataType = outputRowType

  //~ ExecNode methods -----------------------------------------------------------

  override def getInputNodes: util.List[ExecNode[StreamPlanner, _]] =
    List(getInput.asInstanceOf[ExecNode[StreamPlanner, _]])

  override def replaceInputNode(
      ordinalInParent: Int,
      newInputNode: ExecNode[StreamPlanner, _]): Unit = {
    replaceInput(ordinalInParent, newInputNode.asInstanceOf[RelNode])
  }

}
