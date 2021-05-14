package org.apache.flink.table.planner.codegen.agg

import org.apache.flink.table.planner.codegen.{ExprCodeGenerator, GeneratedExpression}

/**
  * The base trait for code generating aggregate operations, such as accumulate and retract.
  * The implementation including declarative and imperative.
  */
trait AggCodeGen {

  def createAccumulator(generator: ExprCodeGenerator): Seq[GeneratedExpression]

  def setAccumulator(generator: ExprCodeGenerator): String

  def getAccumulator(generator: ExprCodeGenerator): Seq[GeneratedExpression]

  def resetAccumulator(generator: ExprCodeGenerator): String

  def accumulate(generator: ExprCodeGenerator): String

  def retract(generator: ExprCodeGenerator): String

  def merge(generator: ExprCodeGenerator): String

  def getValue(generator: ExprCodeGenerator): GeneratedExpression

  def checkNeededMethods(
    needAccumulate: Boolean = false,
    needRetract: Boolean = false,
    needMerge: Boolean = false,
    needReset: Boolean = false,
    needEmitValue: Boolean = false): Unit
}
