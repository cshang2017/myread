package org.apache.flink.table.planner.plan.nodes.logical

import org.apache.flink.table.planner.plan.nodes.FlinkConventions
import org.apache.flink.table.planner.plan.schema.DataStreamTable

import com.google.common.collect.ImmutableList
import org.apache.calcite.plan._
import org.apache.calcite.rel.convert.ConverterRule
import org.apache.calcite.rel.core.TableScan
import org.apache.calcite.rel.hint.RelHint
import org.apache.calcite.rel.logical.LogicalTableScan
import org.apache.calcite.rel.metadata.RelMetadataQuery
import org.apache.calcite.rel.{RelCollation, RelCollationTraitDef, RelNode}

import java.util
import java.util.Collections
import java.util.function.Supplier

/**
  * Sub-class of [[TableScan]] that is a relational operator
  * which returns the contents of a [[DataStreamTable]] in Flink.
  */
class FlinkLogicalDataStreamTableScan(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    table: RelOptTable)
  extends TableScan(cluster, traitSet, Collections.emptyList[RelHint](), table)
  with FlinkLogicalRel {

  override def copy(traitSet: RelTraitSet, inputs: util.List[RelNode]): RelNode = {
    new FlinkLogicalDataStreamTableScan(cluster, traitSet, table)
  }

  override def computeSelfCost(planner: RelOptPlanner, mq: RelMetadataQuery): RelOptCost = {
    val rowCnt = mq.getRowCount(this)
    val rowSize = mq.getAverageRowSize(this)
    planner.getCostFactory.makeCost(rowCnt, rowCnt, rowCnt * rowSize)
  }

}

class FlinkLogicalDataStreamTableScanConverter
  extends ConverterRule(
    classOf[LogicalTableScan],
    Convention.NONE,
    FlinkConventions.LOGICAL,
    "FlinkLogicalDataStreamTableScanConverter") {

  override def matches(call: RelOptRuleCall): Boolean = {
    val scan: TableScan = call.rel(0)
    val dataStreamTable = scan.getTable.unwrap(classOf[DataStreamTable[_]])
    dataStreamTable != null
  }

  def convert(rel: RelNode): RelNode = {
    val scan = rel.asInstanceOf[TableScan]
    FlinkLogicalDataStreamTableScan.create(rel.getCluster, scan.getTable)
  }
}

object FlinkLogicalDataStreamTableScan {
  val CONVERTER = new FlinkLogicalDataStreamTableScanConverter

  def isDataStreamTableScan(scan: TableScan): Boolean = {
    val dataStreamTable = scan.getTable.unwrap(classOf[DataStreamTable[_]])
    dataStreamTable != null
  }

  def create(cluster: RelOptCluster, relOptTable: RelOptTable): FlinkLogicalDataStreamTableScan = {
    val dataStreamTable = relOptTable.unwrap(classOf[DataStreamTable[_]])
    val traitSet = cluster.traitSetOf(FlinkConventions.LOGICAL).replaceIfs(
      RelCollationTraitDef.INSTANCE, new Supplier[util.List[RelCollation]]() {
        def get: util.List[RelCollation] = {
          if (dataStreamTable != null) {
            dataStreamTable.getStatistic.getCollations
          } else {
            ImmutableList.of[RelCollation]
          }
        }
      }).simplify()
    new FlinkLogicalDataStreamTableScan(cluster, traitSet, dataStreamTable)
  }
}
