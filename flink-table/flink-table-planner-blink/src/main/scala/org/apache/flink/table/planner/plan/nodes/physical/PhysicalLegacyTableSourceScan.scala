package org.apache.flink.table.planner.plan.nodes.physical

import org.apache.flink.api.common.io.InputFormat
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.dag.Transformation
import org.apache.flink.core.io.InputSplit
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.table.planner.plan.schema.LegacyTableSourceTable
import org.apache.flink.table.runtime.types.TypeInfoDataTypeConverter.fromDataTypeToTypeInfo
import org.apache.flink.table.sources.{InputFormatTableSource, StreamTableSource, TableSource}

import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.RelWriter
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.TableScan

import scala.collection.JavaConverters._

/**
  * Base physical RelNode to read data from an external source defined by a [[TableSource]].
  */
abstract class PhysicalLegacyTableSourceScan(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    relOptTable: LegacyTableSourceTable[_])
  extends TableScan(cluster, traitSet, relOptTable) {

  // cache table source transformation.
  protected var sourceTransform: Transformation[_] = _

  protected val tableSourceTable: LegacyTableSourceTable[_] =
    relOptTable.unwrap(classOf[LegacyTableSourceTable[_]])

  protected[flink] val tableSource: TableSource[_] = tableSourceTable.tableSource

  override def deriveRowType(): RelDataType = {
    // TableScan row type should always keep same with its
    // interval RelOptTable's row type.
    relOptTable.getRowType
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    super.explainTerms(pw)
      .item("fields", getRowType.getFieldNames.asScala.mkString(", "))
  }

  def createInput[IN](
      env: StreamExecutionEnvironment,
      format: InputFormat[IN, _ <: InputSplit],
      t: TypeInformation[IN]): Transformation[IN]

  def getSourceTransformation(env: StreamExecutionEnvironment): Transformation[_] = {
    if (sourceTransform == null) {
      sourceTransform = tableSource match {
        case format: InputFormatTableSource[_] =>
          // we don't use InputFormatTableSource.getDataStream, because in here we use planner
          // type conversion to support precision of Varchar and something else.
          val typeInfo = fromDataTypeToTypeInfo(format.getProducedDataType)
              .asInstanceOf[TypeInformation[Any]]
          createInput(
            env,
            format.getInputFormat.asInstanceOf[InputFormat[Any, _ <: InputSplit]],
            typeInfo.asInstanceOf[TypeInformation[Any]])
            
        case s: StreamTableSource[_] => s.getDataStream(env).getTransformation
      }
    }
    sourceTransform
  }
}
