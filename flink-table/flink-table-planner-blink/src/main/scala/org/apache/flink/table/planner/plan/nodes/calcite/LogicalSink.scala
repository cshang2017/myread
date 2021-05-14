
package org.apache.flink.table.planner.plan.nodes.calcite

import org.apache.flink.table.catalog.{CatalogTable, ObjectIdentifier}
import org.apache.flink.table.connector.sink.DynamicTableSink

import org.apache.calcite.plan.{Convention, RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.RelNode

import java.util

import scala.collection.JavaConversions._

/**
  * Sub-class of [[Sink]] that is a relational expression
  * which writes out data of input node into a [[DynamicTableSink]].
  * This class corresponds to Calcite logical rel.
  */
final class LogicalSink(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    input: RelNode,
    tableIdentifier: ObjectIdentifier,
    catalogTable: CatalogTable,
    tableSink: DynamicTableSink,
    val staticPartitions: Map[String, String])
  extends Sink(cluster, traitSet, input, tableIdentifier, catalogTable, tableSink) {

  override def copy(traitSet: RelTraitSet, inputs: util.List[RelNode]): RelNode = {
    new LogicalSink(
      cluster, traitSet, inputs.head, tableIdentifier, catalogTable, tableSink, staticPartitions)
  }
}

object LogicalSink {

  def create(
      input: RelNode,
      tableIdentifier: ObjectIdentifier,
      catalogTable: CatalogTable,
      tableSink: DynamicTableSink,
      staticPartitions: Map[String, String] = Map()): LogicalSink = {
    val traits = input.getCluster.traitSetOf(Convention.NONE)
    new LogicalSink(
      input.getCluster,
      traits,
      input,
      tableIdentifier,
      catalogTable,
      tableSink,
      staticPartitions)
  }
}




