
package org.apache.flink.table.planner.plan.utils

import org.apache.flink.api.dag.Transformation
import org.apache.flink.table.api.TableConfig
import org.apache.flink.table.data.{RowData, GenericRowData}
import org.apache.flink.table.planner.calcite.FlinkTypeFactory
import org.apache.flink.table.planner.codegen.CodeGenUtils.{DEFAULT_INPUT1_TERM, GENERIC_ROW}
import org.apache.flink.table.planner.codegen.OperatorCodeGenerator.generateCollect
import org.apache.flink.table.planner.codegen.{CodeGenUtils, CodeGeneratorContext, ExprCodeGenerator, OperatorCodeGenerator}
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode
import org.apache.flink.table.runtime.operators.CodeGenOperatorFactory
import org.apache.flink.table.runtime.types.LogicalTypeDataTypeConverter.fromDataTypeToLogicalType
import org.apache.flink.table.runtime.typeutils.RowDataTypeInfo
import org.apache.flink.table.sources.TableSource
import org.apache.flink.table.types.DataType
import org.apache.flink.table.types.logical.RowType
import org.apache.flink.table.typeutils.TimeIndicatorTypeInfo

import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.TableScan
import org.apache.calcite.rex.RexNode

import scala.collection.JavaConversions._

/**
  * Util for [[TableScan]]s.
  */
object ScanUtil {

  private[flink] def hasTimeAttributeField(indexes: Array[Int]) =
    indexes.contains(TimeIndicatorTypeInfo.ROWTIME_STREAM_MARKER)||
        indexes.contains(TimeIndicatorTypeInfo.ROWTIME_BATCH_MARKER)||
        indexes.contains(TimeIndicatorTypeInfo.PROCTIME_STREAM_MARKER)||
        indexes.contains(TimeIndicatorTypeInfo.PROCTIME_BATCH_MARKER)

  private[flink] def needsConversion(source: TableSource[_]): Boolean = {
    needsConversion(source.getProducedDataType)
  }

  private[flink] def needsConversion(dataType: DataType): Boolean =
    fromDataTypeToLogicalType(dataType) match {
      case _: RowType => !CodeGenUtils.isInternalClass(dataType)
      case _ => true
    }

  private[flink] def convertToInternalRow(
      ctx: CodeGeneratorContext,
      input: Transformation[Any],
      fieldIndexes: Array[Int],
      inputType: DataType,
      outRowType: RelDataType,
      qualifiedName: Seq[String],
      config: TableConfig,
      rowtimeExpr: Option[RexNode] = None,
      beforeConvert: String = "",
      afterConvert: String = ""): Transformation[RowData] = {

    val outputRowType = FlinkTypeFactory.toLogicalRowType(outRowType)

    // conversion
    val convertName = "SourceConversion"
    // type convert
    val inputTerm = DEFAULT_INPUT1_TERM
    val internalInType = fromDataTypeToLogicalType(inputType)
    val (inputTermConverter, inputRowType) = {
      val convertFunc = CodeGenUtils.genToInternal(ctx, inputType)
      internalInType match {
        case rt: RowType => (convertFunc, rt)
        case _ => ((record: String) => s"$GENERIC_ROW.of(${convertFunc(record)})",
            RowType.of(internalInType))
      }
    }

    val processCode =
      if ((inputRowType.getChildren == outputRowType.getChildren) &&
          (inputRowType.getFieldNames == outputRowType.getFieldNames) &&
          !hasTimeAttributeField(fieldIndexes)) {
        s"${generateCollect(inputTerm)}"
      } else {

        // field index change (pojo) or has time attribute field
        val conversion = new ExprCodeGenerator(ctx, false)
            .bindInput(inputRowType, inputTerm = inputTerm, inputFieldMapping = Some(fieldIndexes))
            .generateConverterResultExpression(
              outputRowType, classOf[GenericRowData], rowtimeExpression = rowtimeExpr)

        s"""
           |$beforeConvert
           |${conversion.code}
           |${generateCollect(conversion.resultTerm)}
           |$afterConvert
           |""".stripMargin
      }

    val generatedOperator = OperatorCodeGenerator.generateOneInputStreamOperator[Any, RowData](
      ctx,
      convertName,
      processCode,
      outputRowType,
      converter = inputTermConverter)

    val substituteStreamOperator = new CodeGenOperatorFactory[RowData](generatedOperator)

    ExecNode.createOneInputTransformation(
      input.asInstanceOf[Transformation[RowData]],
      getOperatorName(qualifiedName, outRowType),
      substituteStreamOperator,
      RowDataTypeInfo.of(outputRowType),
      input.getParallelism)
  }

  /**
    * @param qualifiedName qualified name for table
    */
  private[flink] def getOperatorName(qualifiedName: Seq[String], rowType: RelDataType): String = {
    val tableQualifiedName = qualifiedName.mkString(".")
    val fieldNames = rowType.getFieldNames.mkString(", ")
    s"SourceConversion(table=[$tableQualifiedName], fields=[$fieldNames])"
  }
}
