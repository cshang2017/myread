package org.apache.flink.table.planner.plan.utils

import org.apache.calcite.rex._

import java.util.{List => JList}

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

object RexNodeRewriter {

  /**
    * Generates new expressions with used input fields.
    *
    * @param usedFields indices of used input fields
    * @param exps       original expression lists
    * @return new expression with only used input fields
    */
  def rewriteWithNewFieldInput(
      exps: JList[RexNode],
      usedFields: Array[Int]): JList[RexNode] = {
    // rewrite input field in expressions
    val inputRewriter = new InputRewriter(usedFields.zipWithIndex.toMap)
    exps.map(_.accept(inputRewriter)).toList.asJava
  }
}

/**
  * A RexShuttle to rewrite field accesses of RexNode.
  *
  * @param fieldMap old input fields ref index -> new input fields ref index mappings
  */
class InputRewriter(fieldMap: Map[Int, Int]) extends RexShuttle {

  override def visitInputRef(inputRef: RexInputRef): RexNode =
    new RexInputRef(refNewIndex(inputRef), inputRef.getType)

  override def visitLocalRef(localRef: RexLocalRef): RexNode =
    new RexInputRef(refNewIndex(localRef), localRef.getType)

  private def refNewIndex(ref: RexSlot): Int =
    fieldMap.getOrElse(ref.getIndex,
      throw new IllegalArgumentException("input field contains invalid index"))
}
