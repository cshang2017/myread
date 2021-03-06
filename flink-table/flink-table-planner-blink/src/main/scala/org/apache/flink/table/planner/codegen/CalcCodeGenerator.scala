package org.apache.flink.table.planner.codegen

import org.apache.flink.api.common.functions.{FlatMapFunction, Function}
import org.apache.flink.api.dag.Transformation
import org.apache.flink.table.api.{TableConfig, TableException}
import org.apache.flink.table.data.{BoxedWrapperRowData, RowData}
import org.apache.flink.table.runtime.generated.GeneratedFunction
import org.apache.flink.table.runtime.operators.CodeGenOperatorFactory
import org.apache.flink.table.runtime.typeutils.RowDataTypeInfo
import org.apache.flink.table.types.logical.RowType

import org.apache.calcite.plan.RelOptCluster
import org.apache.calcite.rex._

import scala.collection.JavaConversions._

object CalcCodeGenerator {

  private[flink] def generateCalcOperator(
      ctx: CodeGeneratorContext,
      cluster: RelOptCluster,
      inputTransform: Transformation[RowData],
      outputType: RowType,
      config: TableConfig,
      calcProgram: RexProgram,
      condition: Option[RexNode],
      retainHeader: Boolean = false,
      opName: String): CodeGenOperatorFactory[RowData] = {
        
    val inputType = inputTransform.getOutputType.asInstanceOf[RowDataTypeInfo].toRowType
    // filter out time attributes
    val inputTerm = CodeGenUtils.DEFAULT_INPUT1_TERM
    val processCode = generateProcessCode(
      ctx,
      inputType,
      outputType,
      classOf[BoxedWrapperRowData],
      outputType.getFieldNames,
      config,
      calcProgram,
      condition,
      eagerInputUnboxingCode = true,
      retainHeader = retainHeader,
      allowSplit = true)

    val genOperator =
      OperatorCodeGenerator.generateOneInputStreamOperator[RowData, RowData](
        ctx,
        opName,
        processCode,
        inputType,
        inputTerm = inputTerm,
        lazyInputUnboxingCode = true)

    new CodeGenOperatorFactory(genOperator)
  }

  private[flink] def generateFunction[T <: Function](
      inputType: RowType,
      name: String,
      returnType: RowType,
      outRowClass: Class[_ <: RowData],
      calcProjection: RexProgram,
      calcCondition: Option[RexNode],
      config: TableConfig): GeneratedFunction[FlatMapFunction[RowData, RowData]] = {
    val ctx = CodeGeneratorContext(config)
    val inputTerm = CodeGenUtils.DEFAULT_INPUT1_TERM
    val collectorTerm = CodeGenUtils.DEFAULT_COLLECTOR_TERM
    val processCode = generateProcessCode(
      ctx,
      inputType,
      returnType,
      outRowClass,
      returnType.getFieldNames,
      config,
      calcProjection,
      calcCondition,
      collectorTerm = collectorTerm,
      eagerInputUnboxingCode = false,
      outputDirectly = true
    )

    FunctionCodeGenerator.generateFunction(
      ctx,
      name,
      classOf[FlatMapFunction[RowData, RowData]],
      processCode,
      returnType,
      inputType,
      input1Term = inputTerm,
      collectorTerm = collectorTerm)
  }

  private[flink] def generateProcessCode(
      ctx: CodeGeneratorContext,
      inputType: RowType,
      outRowType: RowType,
      outRowClass: Class[_ <: RowData],
      resultFieldNames: Seq[String],
      config: TableConfig,
      calcProgram: RexProgram,
      condition: Option[RexNode],
      inputTerm: String = CodeGenUtils.DEFAULT_INPUT1_TERM,
      collectorTerm: String = CodeGenUtils.DEFAULT_OPERATOR_COLLECTOR_TERM,
      eagerInputUnboxingCode: Boolean,
      retainHeader: Boolean = false,
      outputDirectly: Boolean = false,
      allowSplit: Boolean = false): String = {

    val projection = calcProgram.getProjectList.map(calcProgram.expandLocalRef)
    val exprGenerator = new ExprCodeGenerator(ctx, false)
        .bindInput(inputType, inputTerm = inputTerm)

    val onlyFilter = projection.lengthCompare(inputType.getFieldCount) == 0 &&
      projection.zipWithIndex.forall { case (rexNode, index) =>
        rexNode.isInstanceOf[RexInputRef] && rexNode.asInstanceOf[RexInputRef].getIndex == index
      }

    def produceOutputCode(resultTerm: String) = if (outputDirectly) {
      s"$collectorTerm.collect($resultTerm);"
    } else {
      s"${OperatorCodeGenerator.generateCollect(resultTerm)}"
    }

    def produceProjectionCode = {
      // we cannot use for-loop optimization if projection contains other calculations
      // (for example "select id + 1 from T")
      val simpleProjection = projection.forall { rexNode => rexNode.isInstanceOf[RexInputRef] }

      val projectionExpression = if (simpleProjection) {
        val inputMapping = projection.map(_.asInstanceOf[RexInputRef].getIndex).toArray
        ProjectionCodeGenerator.generateProjectionExpression(
          ctx, inputType, outRowType, inputMapping,
          outRowClass, inputTerm, nullCheck = config.getNullCheck)
      } else {
        val projectionExprs = projection.map(exprGenerator.generateExpression)
        exprGenerator.generateResultExpression(
          projectionExprs,
          outRowType,
          outRowClass,
          allowSplit = allowSplit)
      }

      val projectionExpressionCode = projectionExpression.code

      val header = if (retainHeader) {
        s"${projectionExpression.resultTerm}.setRowKind($inputTerm.getRowKind());"
      } else {
        ""
      }

      s"""
         |$header
         |$projectionExpressionCode
         |${produceOutputCode(projectionExpression.resultTerm)}
         |""".stripMargin
    }

    if (condition.isEmpty && onlyFilter) {
      throw new TableException("This calc has no useful projection and no filter. " +
        "It should be removed by CalcRemoveRule.")
    } else if (condition.isEmpty) { // only projection
      val projectionCode = produceProjectionCode
      s"""
         |${if (eagerInputUnboxingCode) ctx.reuseInputUnboxingCode() else ""}
         |$projectionCode
         |""".stripMargin
    } else {
      val filterCondition = exprGenerator.generateExpression(condition.get)
      // only filter
      if (onlyFilter) {
        s"""
           |${if (eagerInputUnboxingCode) ctx.reuseInputUnboxingCode() else ""}
           |${filterCondition.code}
           |if (${filterCondition.resultTerm}) {
           |  ${produceOutputCode(inputTerm)}
           |}
           |""".stripMargin
      } else { // both filter and projection
        val filterInputCode = ctx.reuseInputUnboxingCode()
        val filterInputSet = Set(ctx.reusableInputUnboxingExprs.keySet.toSeq: _*)

        // if any filter conditions, projection code will enter an new scope
        val projectionCode = produceProjectionCode

        val projectionInputCode = ctx.reusableInputUnboxingExprs
          .filter(entry => !filterInputSet.contains(entry._1))
          .values.map(_.code).mkString("\n")
        s"""
           |${if (eagerInputUnboxingCode) filterInputCode else ""}
           |${filterCondition.code}
           |if (${filterCondition.resultTerm}) {
           |  ${if (eagerInputUnboxingCode) projectionInputCode else ""}
           |  $projectionCode
           |}
           |""".stripMargin
      }
    }
  }
}
