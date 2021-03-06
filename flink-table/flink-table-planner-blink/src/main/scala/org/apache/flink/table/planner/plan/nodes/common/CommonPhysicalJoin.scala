package org.apache.flink.table.planner.plan.nodes.common

import org.apache.flink.table.api.TableException
import org.apache.flink.table.planner.plan.nodes.physical.FlinkPhysicalRel
import org.apache.flink.table.planner.plan.utils.{JoinTypeUtil, JoinUtil}
import org.apache.flink.table.planner.plan.utils.PythonUtil.containsPythonCall
import org.apache.flink.table.planner.plan.utils.RelExplainUtil.preferExpressionFormat
import org.apache.flink.table.runtime.operators.join.FlinkJoinType

import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.`type`.{RelDataType, RelDataTypeField}
import org.apache.calcite.rel.core.{CorrelationId, Join, JoinInfo, JoinRelType}
import org.apache.calcite.rel.{RelNode, RelWriter}
import org.apache.calcite.rex.RexNode
import org.apache.calcite.sql.validate.SqlValidatorUtil
import org.apache.calcite.util.mapping.IntPair

import java.util
import java.util.Collections

import scala.collection.JavaConversions._

/**
  * Base physical class for flink [[Join]].
  */
abstract class CommonPhysicalJoin(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    leftRel: RelNode,
    rightRel: RelNode,
    condition: RexNode,
    joinType: JoinRelType)
  extends Join(cluster, traitSet, leftRel, rightRel, condition, Set.empty[CorrelationId], joinType)
  with FlinkPhysicalRel {

  if (containsPythonCall(condition)) {
    throw new TableException("Only inner join condition with equality predicates supports the " +
      "Python UDF taking the inputs from the left table and the right table at the same time, " +
      "e.g., ON T1.id = T2.id && pythonUdf(T1.a, T2.b)")
  }

  def getJoinInfo: JoinInfo = joinInfo

  lazy val filterNulls: Array[Boolean] = {
    val filterNulls = new util.ArrayList[java.lang.Boolean]
    JoinUtil.createJoinInfo(getLeft, getRight, getCondition, filterNulls)
    filterNulls.map(_.booleanValue()).toArray
  }

  lazy val keyPairs: List[IntPair] = getJoinInfo.pairs.toList

  // TODO remove FlinkJoinType
  lazy val flinkJoinType: FlinkJoinType = JoinTypeUtil.getFlinkJoinType(this.getJoinType)

  lazy val inputRowType: RelDataType = joinType match {
    case JoinRelType.SEMI | JoinRelType.ANTI =>
      // Combines inputs' RowType, the result is different from SEMI/ANTI Join's RowType.
      SqlValidatorUtil.createJoinType(
        getCluster.getTypeFactory,
        getLeft.getRowType,
        getRight.getRowType,
        null,
        Collections.emptyList[RelDataTypeField]
      )
    case _ =>
      getRowType
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    pw.input("left", getLeft).input("right", getRight)
      .item("joinType", flinkJoinType.toString)
      .item("where", getExpressionString(
        getCondition, inputRowType.getFieldNames.toList, None, preferExpressionFormat(pw)))
      .item("select", getRowType.getFieldNames.mkString(", "))
  }

}
