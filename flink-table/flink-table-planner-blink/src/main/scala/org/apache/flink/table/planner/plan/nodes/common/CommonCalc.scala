package org.apache.flink.table.planner.plan.nodes.common

import org.apache.flink.table.planner.plan.nodes.ExpressionFormat.ExpressionFormat
import org.apache.flink.table.planner.plan.nodes.{ExpressionFormat, FlinkRelNode}
import org.apache.flink.table.planner.plan.utils.RelExplainUtil.{conditionToString, preferExpressionFormat}

import org.apache.calcite.plan.{RelOptCluster, RelOptCost, RelOptPlanner, RelTraitSet}
import org.apache.calcite.rel.core.Calc
import org.apache.calcite.rel.hint.RelHint
import org.apache.calcite.rel.metadata.RelMetadataQuery
import org.apache.calcite.rel.{RelNode, RelWriter}
import org.apache.calcite.rex.{RexCall, RexInputRef, RexLiteral, RexProgram}

import java.util.Collections

import scala.collection.JavaConversions._

/**
  * Base class for flink [[Calc]].
  */
abstract class CommonCalc(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    input: RelNode,
    calcProgram: RexProgram)
  extends Calc(cluster, traitSet, Collections.emptyList[RelHint](), input, calcProgram)
  with FlinkRelNode {

  override def computeSelfCost(planner: RelOptPlanner, mq: RelMetadataQuery): RelOptCost = {
    val calcProgram = getProgram
    // compute number of expressions that do not access a field or literal, i.e. computations,
    // conditions, etc. We only want to account for computations, not for simple projections.
    // CASTs in RexProgram are reduced as far as possible by ReduceExpressionsRule
    // in normalization stage. So we should ignore CASTs here in optimization stage.
    val compCnt = calcProgram.getProjectList.map(calcProgram.expandLocalRef).toList.count {
      case _: RexInputRef => false
      case _: RexLiteral => false
      case c: RexCall if c.getOperator.getName.equals("CAST") => false
      case _ => true
    }
    val newRowCnt = mq.getRowCount(this)
    // TODO use inputRowCnt to compute cpu cost
    planner.getCostFactory.makeCost(newRowCnt, newRowCnt * compCnt, 0)
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    pw.input("input", getInput)
      .item("select", projectionToString(preferExpressionFormat(pw)))
      .itemIf("where",
        conditionToString(calcProgram, getExpressionString, preferExpressionFormat(pw)),
        calcProgram.getCondition != null)
  }

  protected def projectionToString(
      expressionFormat: ExpressionFormat = ExpressionFormat.Prefix): String = {
    val projectList = calcProgram.getProjectList.toList
    val inputFieldNames = calcProgram.getInputRowType.getFieldNames.toList
    val localExprs = calcProgram.getExprList.toList
    val outputFieldNames = calcProgram.getOutputRowType.getFieldNames.toList

    projectList
      .map(getExpressionString(_, inputFieldNames, Some(localExprs), expressionFormat))
      .zip(outputFieldNames).map { case (e, o) =>
      if (e != o) {
        e + " AS " + o
      } else {
        e
      }
    }.mkString(", ")
  }

}
