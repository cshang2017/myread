package org.apache.flink.table.planner.plan.schema

import org.apache.flink.table.catalog.{CatalogTable, ObjectIdentifier}
import org.apache.flink.table.connector.source.DynamicTableSource
import org.apache.flink.table.planner.JMap
import org.apache.flink.table.planner.plan.stats.FlinkStatistic

import com.google.common.collect.ImmutableList
import org.apache.calcite.plan.RelOptSchema
import org.apache.calcite.rel.`type`.RelDataType

import java.util

/**
 * A [[FlinkPreparingTableBase]] implementation which defines the context variables
 * required to translate the Calcite [[org.apache.calcite.plan.RelOptTable]] to the Flink specific
 * relational expression with [[DynamicTableSource]].
 *
 * @param relOptSchema The RelOptSchema that this table comes from
 * @param tableIdentifier The full path of the table to retrieve.
 * @param rowType The table row type
 * @param statistic The table statistics
 * @param tableSource The [[DynamicTableSource]] for which is converted to a Calcite Table
 * @param isStreamingMode A flag that tells if the current table is in stream mode
 * @param catalogTable Catalog table where this table source table comes from
 * @param dynamicOptions The dynamic hinted options
 * @param extraDigests The extra digests which will be added into `getQualifiedName`
 *                     as a part of table digest
 */
class TableSourceTable(
    relOptSchema: RelOptSchema,
    val tableIdentifier: ObjectIdentifier,
    rowType: RelDataType,
    statistic: FlinkStatistic,
    val tableSource: DynamicTableSource,
    val isStreamingMode: Boolean,
    val catalogTable: CatalogTable,
    val dynamicOptions: JMap[String, String],
    val extraDigests: Array[String] = Array.empty)
  extends FlinkPreparingTableBase(
    relOptSchema,
    rowType,
    util.Arrays.asList(
      tableIdentifier.getCatalogName,
      tableIdentifier.getDatabaseName,
      tableIdentifier.getObjectName),
    statistic) {

  override def getQualifiedName: util.List[String] = {
    val names = super.getQualifiedName
    val builder = ImmutableList.builder[String]()
    builder.addAll(names)
    if (dynamicOptions.size() != 0) {
      // Add the dynamic options as part of the table digest,
      // this is a temporary solution, we expect to avoid this
      // before Calcite 1.23.0.
      builder.add(s"dynamic options: $dynamicOptions")
    }
    extraDigests.foreach(builder.add)
    builder.build()
  }

  /**
   * Creates a copy of this table with specified digest.
   *
   * @param newTableSource tableSource to replace
   * @param newRowType new row type
   * @return added TableSourceTable instance with specified digest
   */
  def copy(
      newTableSource: DynamicTableSource,
      newRowType: RelDataType,
      newExtraDigests: Array[String]): TableSourceTable = {
    new TableSourceTable(
      relOptSchema,
      tableIdentifier,
      newRowType,
      statistic,
      newTableSource,
      isStreamingMode,
      catalogTable,
      dynamicOptions,
      extraDigests ++ newExtraDigests)
  }
}
