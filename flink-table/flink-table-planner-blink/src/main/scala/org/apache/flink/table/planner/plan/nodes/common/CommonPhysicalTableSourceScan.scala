

package org.apache.flink.table.planner.plan.nodes.common

import org.apache.flink.api.common.io.InputFormat
import org.apache.flink.api.dag.Transformation
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.table.connector.source.{InputFormatProvider, ScanTableSource, SourceFunctionProvider}
import org.apache.flink.table.data.RowData
import org.apache.flink.table.planner.calcite.FlinkTypeFactory
import org.apache.flink.table.planner.plan.schema.TableSourceTable
import org.apache.flink.table.runtime.connector.source.ScanRuntimeProviderContext
import org.apache.flink.table.runtime.typeutils.RowDataTypeInfo

import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.RelWriter
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.TableScan

import scala.collection.JavaConverters._

/**
  * Base physical RelNode to read data from an external source defined by a [[ScanTableSource]].
  */
abstract class CommonPhysicalTableSourceScan(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    relOptTable: TableSourceTable)
  extends TableScan(cluster, traitSet, relOptTable) {

  // cache table source transformation.
  protected var sourceTransform: Transformation[_] = _

  protected val tableSourceTable: TableSourceTable = relOptTable.unwrap(classOf[TableSourceTable])

  protected[flink] val tableSource: ScanTableSource =
    tableSourceTable.tableSource.asInstanceOf[ScanTableSource]

  override def deriveRowType(): RelDataType = {
    // TableScan row type should always keep same with its
    // interval RelOptTable's row type.
    relOptTable.getRowType
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    super.explainTerms(pw)
      .item("fields", getRowType.getFieldNames.asScala.mkString(", "))
  }

  protected def createSourceTransformation(
      env: StreamExecutionEnvironment,
      name: String): Transformation[RowData] = {
    val runtimeProvider = tableSource.getScanRuntimeProvider(ScanRuntimeProviderContext.INSTANCE)
    val outRowType = FlinkTypeFactory.toLogicalRowType(tableSourceTable.getRowType)
    val outTypeInfo = new RowDataTypeInfo(outRowType)

    runtimeProvider match {
      case provider: SourceFunctionProvider =>
        val sourceFunction = provider.createSourceFunction()
        env
          .addSource(sourceFunction, name, outTypeInfo)
          .getTransformation
      case provider: InputFormatProvider =>
        val inputFormat = provider.createInputFormat()
        createInputFormatTransformation(env, inputFormat, name, outTypeInfo)
    }
  }

  /**
   * Creates a [[Transformation]] based on the given [[InputFormat]].
   * The implementation is different for streaming mode and batch mode.
   */
  protected def createInputFormatTransformation(
      env: StreamExecutionEnvironment,
      inputFormat: InputFormat[RowData, _],
      name: String,
      outTypeInfo: RowDataTypeInfo): Transformation[RowData]
}
