

package org.apache.flink.table.planner.codegen.agg.batch

import org.apache.flink.runtime.util.SingleElementIterator
import org.apache.flink.streaming.api.operators.OneInputStreamOperator
import org.apache.flink.table.data.{GenericRowData, RowData}
import org.apache.flink.table.expressions.ApiExpressionUtils.localRef
import org.apache.flink.table.expressions.{Expression, _}
import org.apache.flink.table.functions.{AggregateFunction, UserDefinedFunction}
import org.apache.flink.table.planner.codegen.CodeGenUtils._
import org.apache.flink.table.planner.codegen.OperatorCodeGenerator.STREAM_RECORD
import org.apache.flink.table.planner.codegen._
import org.apache.flink.table.planner.expressions.DeclarativeExpressionResolver
import org.apache.flink.table.planner.expressions.DeclarativeExpressionResolver.toRexInputRef
import org.apache.flink.table.planner.expressions.converter.ExpressionConverter
import org.apache.flink.table.planner.functions.aggfunctions.DeclarativeAggregateFunction
import org.apache.flink.table.planner.functions.utils.UserDefinedFunctionUtils.{getAccumulatorTypeOfAggregateFunction, getAggUserDefinedInputTypes}
import org.apache.flink.table.runtime.context.ExecutionContextImpl
import org.apache.flink.table.runtime.generated.{GeneratedAggsHandleFunction, GeneratedOperator}
import org.apache.flink.table.runtime.types.InternalSerializers
import org.apache.flink.table.runtime.types.LogicalTypeDataTypeConverter.{fromDataTypeToLogicalType, fromLogicalTypeToDataType}
import org.apache.flink.table.types.DataType
import org.apache.flink.table.types.logical.LogicalTypeRoot._
import org.apache.flink.table.types.logical.{DistinctType, LogicalType, RowType}
import org.apache.calcite.rel.core.AggregateCall
import org.apache.calcite.rex.RexNode
import org.apache.calcite.tools.RelBuilder

import scala.annotation.tailrec

/**
  * Batch aggregate code generate helper.
  */
object AggCodeGenHelper {

  def getAggBufferNames(
      auxGrouping: Array[Int], aggregates: Seq[UserDefinedFunction]): Array[Array[String]] = {
    auxGrouping.zipWithIndex.map {
      case (_, index) => Array(s"aux_group$index")
    } ++ aggregates.zipWithIndex.toArray.map {
      case (a: DeclarativeAggregateFunction, index) =>
        val idx = auxGrouping.length + index
        a.aggBufferAttributes.map(attr => s"agg${idx}_${attr.getName}")
      case (_: AggregateFunction[_, _], index) =>
        val idx = auxGrouping.length + index
        Array(s"agg$idx")
    }
  }

  def getAggBufferTypes(
      inputType: RowType, auxGrouping: Array[Int], aggregates: Seq[UserDefinedFunction])
    : Array[Array[LogicalType]] = {
    auxGrouping.map { index =>
      Array(inputType.getTypeAt(index))
    } ++ aggregates.map {
      case a: DeclarativeAggregateFunction => a.getAggBufferTypes.map(_.getLogicalType)
      case a: AggregateFunction[_, _] =>
        Array(fromDataTypeToLogicalType(getAccumulatorTypeOfAggregateFunction(a)))
    }.toArray[Array[LogicalType]]
  }

  def getUdaggs(
      aggregates: Seq[UserDefinedFunction]): Map[AggregateFunction[_, _], String] = {
    aggregates
        .filter(a => a.isInstanceOf[AggregateFunction[_, _]])
        .map(a => a -> CodeGenUtils.udfFieldName(a)).toMap
        .asInstanceOf[Map[AggregateFunction[_, _], String]]
  }

  def projectRowType(rowType: RowType, mapping: Array[Int]): RowType = {
    RowType.of(mapping.map(rowType.getTypeAt), mapping.map(rowType.getFieldNames.get(_)))
  }

  /**
    * Add agg handler to class member and open it.
    */
  private[flink] def addAggsHandler(
      aggsHandler: GeneratedAggsHandleFunction,
      ctx: CodeGeneratorContext,
      aggsHandlerCtx: CodeGeneratorContext): String = {
    ctx.addReusableInnerClass(aggsHandler.getClassName, aggsHandler.getCode)
    val handler = CodeGenUtils.newName("handler")
    ctx.addReusableMember(s"${aggsHandler.getClassName} $handler = null;")
    val aggRefers = ctx.addReusableObject(aggsHandlerCtx.references.toArray, "Object[]")
    ctx.addReusableOpenStatement(
      s"""
         |$handler = new ${aggsHandler.getClassName}($aggRefers);
         |$handler.open(new ${classOf[ExecutionContextImpl].getCanonicalName}(
         |  this, getRuntimeContext()));
       """.stripMargin)
    ctx.addReusableCloseStatement(s"$handler.close();")
    handler
  }

  /**
    * The generated codes only supports the comparison of the key terms
    * in the form of binary row with only one memory segment.
    */
  private[flink] def genGroupKeyChangedCheckCode(
      currentKeyTerm: String,
      lastKeyTerm: String): String = {
    s"""
       |$currentKeyTerm.getSizeInBytes() != $lastKeyTerm.getSizeInBytes() ||
       |  !(org.apache.flink.table.data.binary.BinaryRowDataUtil.byteArrayEquals(
       |     $currentKeyTerm.getSegments()[0].getHeapMemory(),
       |     $lastKeyTerm.getSegments()[0].getHeapMemory(),
       |     $currentKeyTerm.getSizeInBytes()))
       """.stripMargin.trim
  }

  def genSortAggCodes(
      isMerge: Boolean,
      isFinal: Boolean,
      ctx: CodeGeneratorContext,
      builder: RelBuilder,
      grouping: Array[Int],
      auxGrouping: Array[Int],
      aggCallToAggFunction: Seq[(AggregateCall, UserDefinedFunction)],
      aggArgs: Array[Array[Int]],
      aggregates: Seq[UserDefinedFunction],
      aggResultTypes: Seq[DataType],
      udaggs: Map[AggregateFunction[_, _], String],
      inputTerm: String,
      inputType: RowType,
      aggBufferNames: Array[Array[String]],
      aggBufferTypes: Array[Array[LogicalType]],
      outputType: RowType,
      forHashAgg: Boolean = false): (String, String, GeneratedExpression) = {
    // gen code to apply aggregate functions to grouping elements
    val argsMapping = buildAggregateArgsMapping(
      isMerge, grouping.length, inputType, auxGrouping, aggArgs, aggBufferTypes)

    val aggBufferExprs = genFlatAggBufferExprs(
      isMerge,
      ctx,
      builder,
      auxGrouping,
      aggregates,
      argsMapping,
      aggBufferNames,
      aggBufferTypes)

    val initAggBufferCode = genInitFlatAggregateBuffer(
      ctx,
      builder,
      inputType,
      inputTerm,
      grouping,
      auxGrouping,
      aggregates,
      udaggs,
      aggBufferExprs,
      forHashAgg)

    val doAggregateCode = genAggregateByFlatAggregateBuffer(
      isMerge,
      ctx,
      builder,
      inputType,
      inputTerm,
      auxGrouping,
      aggCallToAggFunction,
      aggregates,
      udaggs,
      argsMapping,
      aggBufferNames,
      aggBufferTypes,
      aggBufferExprs)

    val aggOutputExpr = genSortAggOutputExpr(
      isMerge,
      isFinal,
      ctx,
      builder,
      grouping,
      auxGrouping,
      aggregates,
      aggResultTypes,
      udaggs,
      argsMapping,
      aggBufferNames,
      aggBufferTypes,
      aggBufferExprs,
      outputType)

    (initAggBufferCode, doAggregateCode, aggOutputExpr)
  }

  /**
    * Build an arg mapping for reference binding. The mapping will be a 2-dimension array.
    * The first dimension represents the aggregate index, the order is same with agg calls in plan.
    * The second dimension information represents input count of the aggregate. The meaning will
    * be different depends on whether we should do merge.
    *
    * In non-merge case, aggregate functions will treat inputs as operands. In merge case, the
    * input is local aggregation's buffer, we need to merge with our local aggregate buffers.
    */
  private[flink] def buildAggregateArgsMapping(
      isMerge: Boolean,
      aggBufferOffset: Int,
      inputType: RowType,
      auxGrouping: Array[Int],
      aggArgs: Array[Array[Int]],
      aggBufferTypes: Array[Array[LogicalType]]): Array[Array[(Int, LogicalType)]] = {
    val auxGroupingMapping = auxGrouping.indices.map {
      i => Array[(Int, LogicalType)]((i, aggBufferTypes(i)(0)))
    }.toArray

    val aggCallMapping = if (isMerge) {
      var offset = aggBufferOffset + auxGrouping.length
      aggBufferTypes.slice(auxGrouping.length, aggBufferTypes.length).map { types =>
        val baseOffset = offset
        offset = offset + types.length
        types.indices.map(index => (baseOffset + index, types(index))).toArray
      }
    } else {
      aggArgs.map(args => args.map(i => (i, inputType.getTypeAt(i))))
    }

    auxGroupingMapping ++ aggCallMapping
  }

  def newLocalReference(resultTerm: String, resultType: LogicalType): LocalReferenceExpression = {
    localRef(resultTerm, fromLogicalTypeToDataType(resultType))
  }

  /**
    * Resolves the given expression to a resolved Expression.
    *
    * @param isMerge this is called from merge() method
    */
  private case class ResolveReference(
      ctx: CodeGeneratorContext,
      relBuilder: RelBuilder,
      isMerge: Boolean,
      agg: DeclarativeAggregateFunction,
      aggIndex: Int,
      argsMapping: Array[Array[(Int, LogicalType)]],
      aggBufferTypes: Array[Array[LogicalType]])
    extends DeclarativeExpressionResolver(relBuilder, agg, isMerge) {

    override def toMergeInputExpr(name: String, localIndex: Int): ResolvedExpression = {
      val (inputIndex, inputType) = argsMapping(aggIndex)(localIndex)
      toRexInputRef(relBuilder, inputIndex, inputType)
    }

    override def toAccInputExpr(name: String, localIndex: Int): ResolvedExpression = {
      val (inputIndex, inputType) = argsMapping(aggIndex)(localIndex)
      toRexInputRef(relBuilder, inputIndex, inputType)
    }

    override def toAggBufferExpr(name: String, localIndex: Int): ResolvedExpression = {
      val variableName = s"agg${aggIndex}_$name"
      newLocalReference(variableName, aggBufferTypes(aggIndex)(localIndex))
    }
  }

  /**
    * Declare all aggregate buffer variables, store these variables in class members
    */
  private[flink] def genFlatAggBufferExprs(
      isMerge: Boolean,
      ctx: CodeGeneratorContext,
      builder: RelBuilder,
      auxGrouping: Array[Int],
      aggregates: Seq[UserDefinedFunction],
      argsMapping: Array[Array[(Int, LogicalType)]],
      aggBufferNames: Array[Array[String]],
      aggBufferTypes: Array[Array[LogicalType]]): Seq[GeneratedExpression] = {
    val exprCodegen = new ExprCodeGenerator(ctx, false)
    val converter = new ExpressionConverter(builder)

    val accessAuxGroupingExprs = auxGrouping.indices.map {
      idx => newLocalReference(aggBufferNames(idx)(0), aggBufferTypes(idx)(0))
    }.map(_.accept(converter)).map(exprCodegen.generateExpression)

    val aggCallExprs = aggregates.zipWithIndex.flatMap {
      case (agg: DeclarativeAggregateFunction, aggIndex: Int) =>
        val idx = auxGrouping.length + aggIndex
        agg.aggBufferAttributes.map(_.accept(
          ResolveReference(ctx, builder, isMerge, agg, idx, argsMapping, aggBufferTypes)))
      case (_: AggregateFunction[_, _], aggIndex: Int) =>
        val idx = auxGrouping.length + aggIndex
        val variableName = aggBufferNames(idx)(0)
        Some(newLocalReference(variableName, aggBufferTypes(idx)(0)))
    }.map(_.accept(converter)).map(exprCodegen.generateExpression)

    accessAuxGroupingExprs ++ aggCallExprs
  }

  /**
    * Generate codes which will init the aggregate buffer.
    */
  private[flink] def genInitFlatAggregateBuffer(
      ctx: CodeGeneratorContext,
      builder: RelBuilder,
      inputType: RowType,
      inputTerm: String,
      grouping: Array[Int],
      auxGrouping: Array[Int],
      aggregates: Seq[UserDefinedFunction],
      udaggs: Map[AggregateFunction[_, _], String],
      aggBufferExprs: Seq[GeneratedExpression],
      forHashAgg: Boolean = false): String = {
    val exprCodegen = new ExprCodeGenerator(ctx, false)
        .bindInput(inputType, inputTerm = inputTerm, inputFieldMapping = Some(auxGrouping))

    val initAuxGroupingExprs = {
      if (forHashAgg) {
        // access fallbackInput
        auxGrouping.indices.map(idx => idx + grouping.length).toArray
      } else {
        // access input
        auxGrouping
      }
    }.map { idx =>
      GenerateUtils.generateFieldAccess(ctx, inputType, inputTerm, idx)
    }

    val initAggCallBufferExprs = aggregates.flatMap {
      case (agg: DeclarativeAggregateFunction) =>
        agg.initialValuesExpressions
      case (agg: AggregateFunction[_, _]) =>
        Some(agg)
    }.map {
      case (expr: Expression) => expr.accept(new ExpressionConverter(builder))
      case t@_ => t
    }.map {
      case (rex: RexNode) => exprCodegen.generateExpression(rex)
      case (agg: AggregateFunction[_, _]) =>
        val resultTerm = s"${udaggs(agg)}.createAccumulator()"
        val nullTerm = "false"
        val resultType = getAccumulatorTypeOfAggregateFunction(agg)
        GeneratedExpression(
          genToInternal(ctx, resultType, resultTerm),
          nullTerm,
          "",
          fromDataTypeToLogicalType(resultType))
    }

    val initAggBufferExprs = initAuxGroupingExprs ++ initAggCallBufferExprs
    require(aggBufferExprs.length == initAggBufferExprs.length)

    aggBufferExprs.zip(initAggBufferExprs).map {
      case (aggBufVar, initExpr) =>
        val resultCode = genElementCopyTerm(ctx, aggBufVar.resultType, initExpr.resultTerm)
        s"""
           |${initExpr.code}
           |${aggBufVar.nullTerm} = ${initExpr.nullTerm};
           |${aggBufVar.resultTerm} = $resultCode;
         """.stripMargin.trim
    } mkString "\n"
  }

  @tailrec
  private def genElementCopyTerm(
      ctx: CodeGeneratorContext,
      t: LogicalType,
      inputTerm: String)
  : String = t.getTypeRoot match {
    case CHAR | VARCHAR | ARRAY | MULTISET | MAP | ROW | STRUCTURED_TYPE =>
      val serializer = InternalSerializers.create(t)
      val term = ctx.addReusableObject(
        serializer, "serializer", serializer.getClass.getCanonicalName)
      val typeTerm = boxedTypeTermForType(t)
      s"($typeTerm) $term.copy($inputTerm)"
    case DISTINCT_TYPE =>
      genElementCopyTerm(ctx, t.asInstanceOf[DistinctType].getSourceType, inputTerm)
    case _ => inputTerm
  }

  private[flink] def genAggregateByFlatAggregateBuffer(
      isMerge: Boolean,
      ctx: CodeGeneratorContext,
      builder: RelBuilder,
      inputType: RowType,
      inputTerm: String,
      auxGrouping: Array[Int],
      aggCallToAggFunction: Seq[(AggregateCall, UserDefinedFunction)],
      aggregates: Seq[UserDefinedFunction],
      udaggs: Map[AggregateFunction[_, _], String],
      argsMapping: Array[Array[(Int, LogicalType)]],
      aggBufferNames: Array[Array[String]],
      aggBufferTypes: Array[Array[LogicalType]],
      aggBufferExprs: Seq[GeneratedExpression]): String = {
    if (isMerge) {
      genMergeFlatAggregateBuffer(
        ctx,
        builder,
        inputTerm,
        inputType,
        auxGrouping,
        aggregates,
        udaggs,
        argsMapping,
        aggBufferNames,
        aggBufferTypes,
        aggBufferExprs)
    } else {
      genAccumulateFlatAggregateBuffer(
        ctx,
        builder,
        inputTerm,
        inputType,
        auxGrouping,
        aggCallToAggFunction,
        udaggs,
        argsMapping,
        aggBufferNames,
        aggBufferTypes,
        aggBufferExprs)
    }
  }

  def genSortAggOutputExpr(
      isMerge: Boolean,
      isFinal: Boolean,
      ctx: CodeGeneratorContext,
      builder: RelBuilder,
      grouping: Array[Int],
      auxGrouping: Array[Int],
      aggregates: Seq[UserDefinedFunction],
      aggResultTypes: Seq[DataType],
      udaggs: Map[AggregateFunction[_, _], String],
      argsMapping: Array[Array[(Int, LogicalType)]],
      aggBufferNames: Array[Array[String]],
      aggBufferTypes: Array[Array[LogicalType]],
      aggBufferExprs: Seq[GeneratedExpression],
      outputType: RowType): GeneratedExpression = {
    val valueRow = CodeGenUtils.newName("valueRow")
    val resultCodegen = new ExprCodeGenerator(ctx, false)
    if (isFinal) {
      val getValueExprs = genGetValueFromFlatAggregateBuffer(
        isMerge,
        ctx,
        builder,
        auxGrouping,
        aggregates,
        aggResultTypes,
        udaggs,
        argsMapping,
        aggBufferNames,
        aggBufferTypes,
        outputType)
      val valueRowType = RowType.of(getValueExprs.map(_.resultType): _*)
      resultCodegen.generateResultExpression(
        getValueExprs, valueRowType, classOf[GenericRowData], valueRow)
    } else {
      val valueRowType = RowType.of(aggBufferExprs.map(_.resultType): _*)
      resultCodegen.generateResultExpression(
        aggBufferExprs, valueRowType, classOf[GenericRowData], valueRow)
    }
  }

  /**
    * Generate expressions which will get final aggregate value from aggregate buffers.
    */
  private[flink] def genGetValueFromFlatAggregateBuffer(
      isMerge: Boolean,
      ctx: CodeGeneratorContext,
      builder: RelBuilder,
      auxGrouping: Array[Int],
      aggregates: Seq[UserDefinedFunction],
      aggResultTypes: Seq[DataType],
      udaggs: Map[AggregateFunction[_, _], String],
      argsMapping: Array[Array[(Int, LogicalType)]],
      aggBufferNames: Array[Array[String]],
      aggBufferTypes: Array[Array[LogicalType]],
      outputType: RowType): Seq[GeneratedExpression] = {
    val exprCodegen = new ExprCodeGenerator(ctx, false)

    val auxGroupingExprs = auxGrouping.indices.map { idx =>
      val resultTerm = aggBufferNames(idx)(0)
      val nullTerm = s"${resultTerm}IsNull"
      GeneratedExpression(resultTerm, nullTerm, "", aggBufferTypes(idx)(0))
    }

    val aggExprs = aggregates.zipWithIndex.map {
      case (agg: DeclarativeAggregateFunction, aggIndex) =>
        val idx = auxGrouping.length + aggIndex
        agg.getValueExpression.accept(ResolveReference(
          ctx, builder, isMerge, agg, idx, argsMapping, aggBufferTypes))
      case (agg: AggregateFunction[_, _], aggIndex) =>
        val idx = auxGrouping.length + aggIndex
        (agg, idx)
    }.map {
      case (expr: Expression) => expr.accept(new ExpressionConverter(builder))
      case t@_ => t
    }.map {
      case (rex: RexNode) => exprCodegen.generateExpression(rex)
      case (agg: AggregateFunction[_, _], aggIndex: Int) =>
        val resultType = aggResultTypes(aggIndex - auxGrouping.length)
        val accType = getAccumulatorTypeOfAggregateFunction(agg)
        val resultTerm = genToInternal(ctx, resultType,
          s"${udaggs(agg)}.getValue(${genToExternal(ctx, accType, aggBufferNames(aggIndex)(0))})")
        val nullTerm = s"${aggBufferNames(aggIndex)(0)}IsNull"
        GeneratedExpression(resultTerm, nullTerm, "", fromDataTypeToLogicalType(resultType))
    }

    auxGroupingExprs ++ aggExprs
  }

  /**
    * Generate codes which will read input and merge the aggregate buffers.
    */
  private[flink] def genMergeFlatAggregateBuffer(
      ctx: CodeGeneratorContext,
      builder: RelBuilder,
      inputTerm: String,
      inputType: RowType,
      auxGrouping: Array[Int],
      aggregates: Seq[UserDefinedFunction],
      udaggs: Map[AggregateFunction[_, _], String],
      argsMapping: Array[Array[(Int, LogicalType)]],
      aggBufferNames: Array[Array[String]],
      aggBufferTypes: Array[Array[LogicalType]],
      aggBufferExprs: Seq[GeneratedExpression]): String = {
    val exprCodegen = new ExprCodeGenerator(ctx, false).bindInput(inputType, inputTerm = inputTerm)

    // flat map to get flat agg buffers.
    aggregates.zipWithIndex.flatMap {
      case (agg: DeclarativeAggregateFunction, aggIndex) =>
        val idx = auxGrouping.length + aggIndex
        agg.mergeExpressions.map(_.accept(ResolveReference(
          ctx, builder, isMerge = true, agg, idx, argsMapping, aggBufferTypes)))
      case (agg: AggregateFunction[_, _], aggIndex) =>
        val idx = auxGrouping.length + aggIndex
        Some(agg, idx)
    }.zip(aggBufferExprs.slice(auxGrouping.length, aggBufferExprs.size)).map {
      // DeclarativeAggregateFunction
      case ((expr: Expression), aggBufVar) =>
        val mergeExpr = exprCodegen.generateExpression(
          expr.accept(new ExpressionConverter(builder)))
        s"""
           |${mergeExpr.code}
           |${aggBufVar.nullTerm} = ${mergeExpr.nullTerm};
           |if (!${mergeExpr.nullTerm}) {
           |  ${mergeExpr.copyResultTermToTargetIfChanged(ctx, aggBufVar.resultTerm)}
           |}
           """.stripMargin.trim
      // UserDefinedAggregateFunction
      case ((agg: AggregateFunction[_, _], aggIndex: Int), aggBufVar) =>
        val (inputIndex, inputType) = argsMapping(aggIndex)(0)
        val inputRef = toRexInputRef(builder, inputIndex, inputType)
        val inputExpr = exprCodegen.generateExpression(
          inputRef.accept(new ExpressionConverter(builder)))
        val singleIterableClass = classOf[SingleElementIterator[_]].getCanonicalName

        val externalAccT = getAccumulatorTypeOfAggregateFunction(agg)
        val javaField = typeTerm(externalAccT.getConversionClass)
        val tmpAcc = newName("tmpAcc")
        s"""
           |final $singleIterableClass accIt$aggIndex = new  $singleIterableClass();
           |accIt$aggIndex.set(${genToExternal(ctx, externalAccT, inputExpr.resultTerm)});
           |$javaField $tmpAcc = ${genToExternal(ctx, externalAccT, aggBufferNames(aggIndex)(0))};
           |${udaggs(agg)}.merge($tmpAcc, accIt$aggIndex);
           |${aggBufferNames(aggIndex)(0)} = ${genToInternal(ctx, externalAccT, tmpAcc)};
           |${aggBufVar.nullTerm} = ${aggBufferNames(aggIndex)(0)}IsNull || ${inputExpr.nullTerm};
         """.stripMargin
    } mkString "\n"
  }

  /**
    * Generate codes which will read input and accumulating aggregate buffers.
    */
  private[flink] def genAccumulateFlatAggregateBuffer(
      ctx: CodeGeneratorContext,
      builder: RelBuilder,
      inputTerm: String,
      inputType: RowType,
      auxGrouping: Array[Int],
      aggCallToAggFunction: Seq[(AggregateCall, UserDefinedFunction)],
      udaggs: Map[AggregateFunction[_, _], String],
      argsMapping: Array[Array[(Int, LogicalType)]],
      aggBufferNames: Array[Array[String]],
      aggBufferTypes: Array[Array[LogicalType]],
      aggBufferExprs: Seq[GeneratedExpression]): String = {
    val exprCodegen = new ExprCodeGenerator(ctx, false).bindInput(inputType, inputTerm = inputTerm)

    // flat map to get flat agg buffers.
    aggCallToAggFunction.zipWithIndex.flatMap {
      case (aggCallToAggFun, aggIndex) =>
        val idx = auxGrouping.length + aggIndex
        val aggCall = aggCallToAggFun._1
        aggCallToAggFun._2 match {
          case agg: DeclarativeAggregateFunction =>
            agg.accumulateExpressions.map(_.accept(ResolveReference(
              ctx, builder, isMerge = false, agg, idx, argsMapping, aggBufferTypes)))
                .map(e => (e, aggCall))
          case agg: AggregateFunction[_, _] =>
            val idx = auxGrouping.length + aggIndex
            Some(agg, idx, aggCall)
        }
    }.zip(aggBufferExprs.slice(auxGrouping.length, aggBufferExprs.size)).map {
      // DeclarativeAggregateFunction
      case ((expr: Expression, aggCall: AggregateCall), aggBufVar) =>
        val accExpr = exprCodegen.generateExpression(expr.accept(new ExpressionConverter(builder)))
        (s"""
            |${accExpr.code}
            |${aggBufVar.nullTerm} = ${accExpr.nullTerm};
            |if (!${accExpr.nullTerm}) {
            |  ${accExpr.copyResultTermToTargetIfChanged(ctx, aggBufVar.resultTerm)}
            |}
           """.stripMargin, aggCall.filterArg)
      // UserDefinedAggregateFunction
      case ((agg: AggregateFunction[_, _], aggIndex: Int, aggCall: AggregateCall),
      aggBufVar) =>
        val inFields = argsMapping(aggIndex)
        val externalAccType = getAccumulatorTypeOfAggregateFunction(agg)

        val inputExprs = inFields.map {
          f =>
            val inputRef = toRexInputRef(builder, f._1, f._2)
            exprCodegen.generateExpression(inputRef.accept(new ExpressionConverter(builder)))
        }

        val externalUDITypes = getAggUserDefinedInputTypes(
          agg, externalAccType, inputExprs.map(_.resultType))
        val parameters = inputExprs.zipWithIndex.map {
          case (expr, i) =>
            genToExternalIfNeeded(ctx, externalUDITypes(i), expr)
        }

        val javaTerm = typeTerm(externalAccType.getConversionClass)
        val tmpAcc = newName("tmpAcc")
        val innerCode =
          s"""
             |  $javaTerm $tmpAcc = ${
            genToExternal(ctx, externalAccType, aggBufferNames(aggIndex)(0))};
             |  ${udaggs(agg)}.accumulate($tmpAcc, ${parameters.mkString(", ")});
             |  ${aggBufferNames(aggIndex)(0)} = ${genToInternal(ctx, externalAccType, tmpAcc)};
             |  ${aggBufVar.nullTerm} = false;
           """.stripMargin
        (innerCode, aggCall.filterArg)
    }.map({
      case (innerCode, filterArg) =>
        if (filterArg >= 0) {
          s"""
             |if ($inputTerm.getBoolean($filterArg)) {
             | $innerCode
             |}
          """.stripMargin
        } else {
          innerCode
        }
    }) mkString "\n"
  }

  /**
    * Generate a operator with adding a hasInput field to agg operator.
    */
  private[flink] def generateOperator(
      ctx: CodeGeneratorContext,
      name: String,
      operatorBaseClass: String,
      processCode: String,
      endInputCode: String,
      inputType: RowType): GeneratedOperator[OneInputStreamOperator[RowData, RowData]] = {
    ctx.addReusableMember("private boolean hasInput = false;")
    ctx.addReusableMember(s"$STREAM_RECORD element = new $STREAM_RECORD((Object)null);")
    OperatorCodeGenerator.generateOneInputStreamOperator(
      ctx,
      name,
      processCode,
      inputType,
      endInputCode = Some(endInputCode),
      lazyInputUnboxingCode = true)
  }
}
