

package org.apache.flink.table.planner.codegen

import org.apache.flink.table.api.TableConfig
import org.apache.flink.table.data.{GenericRowData, RowData}
import org.apache.flink.table.planner.calcite.FlinkTypeFactory
import org.apache.flink.table.runtime.operators.values.ValuesInputFormat
import org.apache.flink.table.runtime.typeutils.RowDataTypeInfo

import com.google.common.collect.ImmutableList
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rex.RexLiteral

import scala.collection.JavaConversions._

object ValuesCodeGenerator {

  def generatorInputFormat(
    config: TableConfig,
    rowType: RelDataType,
    tuples: ImmutableList[ImmutableList[RexLiteral]],
    description: String): ValuesInputFormat = {
    val outputType = FlinkTypeFactory.toLogicalRowType(rowType)

    val ctx = CodeGeneratorContext(config)
    val exprGenerator = new ExprCodeGenerator(ctx, false)
    // generate code for every record
    val generatedRecords = tuples.map { r =>
      exprGenerator.generateResultExpression(
        r.map(exprGenerator.generateExpression), outputType, classOf[GenericRowData])
    }

    // generate input format
    val generatedFunction = InputFormatCodeGenerator.generateValuesInputFormat[RowData](
      ctx,
      description,
      generatedRecords.map(_.code),
      outputType)

    new ValuesInputFormat(generatedFunction, RowDataTypeInfo.of(outputType))
  }

}
