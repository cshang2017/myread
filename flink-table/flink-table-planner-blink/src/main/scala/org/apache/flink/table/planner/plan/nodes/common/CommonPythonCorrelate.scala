package org.apache.flink.table.planner.plan.nodes.common

import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.JoinRelType
import org.apache.calcite.rex.{RexCall, RexInputRef, RexNode}
import org.apache.flink.api.dag.Transformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.operators.OneInputStreamOperator
import org.apache.flink.streaming.api.transformations.OneInputTransformation
import org.apache.flink.table.data.RowData
import org.apache.flink.table.functions.python.PythonFunctionInfo
import org.apache.flink.table.planner.calcite.FlinkTypeFactory
import org.apache.flink.table.planner.plan.nodes.common.CommonPythonCorrelate.PYTHON_TABLE_FUNCTION_OPERATOR_NAME
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalTableFunctionScan
import org.apache.flink.table.runtime.typeutils.RowDataTypeInfo
import org.apache.flink.table.types.logical.RowType

import scala.collection.mutable

trait CommonPythonCorrelate extends CommonPythonBase {
  private def getPythonTableFunctionOperator(
      config: Configuration,
      inputRowType: RowDataTypeInfo,
      outputRowType: RowDataTypeInfo,
      pythonFunctionInfo: PythonFunctionInfo,
      udtfInputOffsets: Array[Int],
      joinType: JoinRelType): OneInputStreamOperator[RowData, RowData] = {
    val clazz = loadClass(PYTHON_TABLE_FUNCTION_OPERATOR_NAME)
    val ctor = clazz.getConstructor(
      classOf[Configuration],
      classOf[PythonFunctionInfo],
      classOf[RowType],
      classOf[RowType],
      classOf[Array[Int]],
      classOf[JoinRelType])
    ctor.newInstance(
      config,
      pythonFunctionInfo,
      inputRowType.toRowType,
      outputRowType.toRowType,
      udtfInputOffsets,
      joinType)
      .asInstanceOf[OneInputStreamOperator[RowData, RowData]]
  }

  private def extractPythonTableFunctionInfo(
      pythonRexCall: RexCall): (Array[Int], PythonFunctionInfo) = {
    val inputNodes = new mutable.LinkedHashMap[RexNode, Integer]()
    val pythonTableFunctionInfo = createPythonFunctionInfo(pythonRexCall, inputNodes)
    val udtfInputOffsets = inputNodes.toArray
      .map(_._1)
      .collect { case inputRef: RexInputRef => inputRef.getIndex }
    (udtfInputOffsets, pythonTableFunctionInfo)
  }

  protected def createPythonOneInputTransformation(
      inputTransform: Transformation[RowData],
      scan: FlinkLogicalTableFunctionScan,
      name: String,
      outputRowType: RelDataType,
      config: Configuration,
      joinType: JoinRelType): OneInputTransformation[RowData, RowData] = {
    val pythonTableFuncRexCall = scan.getCall.asInstanceOf[RexCall]
    val (pythonUdtfInputOffsets, pythonFunctionInfo) =
      extractPythonTableFunctionInfo(pythonTableFuncRexCall)
    val pythonOperatorInputRowType = inputTransform.getOutputType.asInstanceOf[RowDataTypeInfo]
    val pythonOperatorOutputRowType = RowDataTypeInfo.of(
      FlinkTypeFactory.toLogicalType(outputRowType).asInstanceOf[RowType])
    val pythonOperator = getPythonTableFunctionOperator(
      config,
      pythonOperatorInputRowType,
      pythonOperatorOutputRowType,
      pythonFunctionInfo,
      pythonUdtfInputOffsets,
      joinType)

    new OneInputTransformation(
      inputTransform,
      name,
      pythonOperator,
      pythonOperatorOutputRowType,
      inputTransform.getParallelism)
  }
}

object CommonPythonCorrelate {
  val PYTHON_TABLE_FUNCTION_OPERATOR_NAME =
    "org.apache.flink.table.runtime.operators.python.table.RowDataPythonTableFunctionOperator"
}
