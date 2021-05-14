package org.apache.flink.table.planner.plan.nodes.calcite

import org.apache.flink.table.catalog.{CatalogTable, ObjectIdentifier}
import org.apache.flink.table.connector.sink.DynamicTableSink
import org.apache.flink.table.planner.calcite.FlinkTypeFactory
import org.apache.flink.table.runtime.types.LogicalTypeDataTypeConverter.fromDataTypeToLogicalType

import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.{RelNode, RelWriter, SingleRel}

import scala.collection.JavaConversions._

/**
  * Relational expression that writes out data of input node into a [[DynamicTableSink]].
  *
  * @param cluster  cluster that this relational expression belongs to
  * @param traitSet the traits of this rel
  * @param input    input relational expression
 *  @param tableIdentifier the full path of the table to retrieve.
 *  @param catalogTable Catalog table where this table source table comes from
 *  @param tableSink the [[DynamicTableSink]] for which to write into
  */
abstract class Sink(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    input: RelNode,
    val tableIdentifier: ObjectIdentifier,
    val catalogTable: CatalogTable,
    val tableSink: DynamicTableSink)
  extends SingleRel(cluster, traitSet, input) {

  override def deriveRowType(): RelDataType = {
    val typeFactory = getCluster.getTypeFactory.asInstanceOf[FlinkTypeFactory]
    val outputType = catalogTable.getSchema.toPhysicalRowDataType
    typeFactory.createFieldTypeFromLogicalType(fromDataTypeToLogicalType(outputType))
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    super.explainTerms(pw)
      .item("table", tableIdentifier.asSummaryString())
      .item("fields", getRowType.getFieldNames.mkString(", "))
  }
}
