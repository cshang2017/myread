

package org.apache.flink.table.planner.plan.nodes.common

import org.apache.flink.table.planner.plan.nodes.FlinkRelNode
import org.apache.flink.table.planner.plan.schema.IntermediateRelTable

import org.apache.calcite.plan.{RelOptCluster, RelOptCost, RelOptPlanner, RelOptTable, RelTraitSet}
import org.apache.calcite.rel.RelWriter
import org.apache.calcite.rel.core.TableScan
import org.apache.calcite.rel.metadata.RelMetadataQuery

import scala.collection.JavaConversions._

/**
  * Base class that wraps [[IntermediateRelTable]].
  */
abstract class CommonIntermediateTableScan(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    table: RelOptTable)
  extends TableScan(cluster, traitSet, table)
  with FlinkRelNode {

  val intermediateTable: IntermediateRelTable = getTable.unwrap(classOf[IntermediateRelTable])

  override def getRelDetailedDescription: String = {
    intermediateTable.relNode.asInstanceOf[FlinkRelNode].getRelDetailedDescription
  }

  override def computeSelfCost(planner: RelOptPlanner, mq: RelMetadataQuery): RelOptCost = {
    val rowCnt = mq.getRowCount(this)
    val rowSize = mq.getAverageRowSize(this)
    planner.getCostFactory.makeCost(rowCnt, rowCnt, rowCnt * rowSize)
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    super.explainTerms(pw)
      .item("fields", getRowType.getFieldNames.mkString(", "))
  }
}

