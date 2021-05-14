package org.apache.flink.table.planner.plan.rules.logical

import org.apache.flink.table.planner.calcite.FlinkRelFactories
import org.apache.flink.table.planner.plan.nodes.calcite.LogicalWindowAggregate

import org.apache.calcite.plan.RelOptRule._
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.{Aggregate, AggregateCall}
import org.apache.calcite.rel.logical.LogicalAggregate
import org.apache.calcite.rel.rules.AggregateReduceFunctionsRule
import org.apache.calcite.rex.RexNode
import org.apache.calcite.tools.RelBuilder

import java.util

import scala.collection.JavaConversions._

/**
  * Rule to convert complex aggregation functions into simpler ones.
  * Have a look at [[AggregateReduceFunctionsRule]] for details.
  */
class WindowAggregateReduceFunctionsRule
  extends AggregateReduceFunctionsRule(
    operand(classOf[LogicalWindowAggregate], any()),
    FlinkRelFactories.LOGICAL_BUILDER_WITHOUT_AGG_INPUT_PRUNE) {

  override def newAggregateRel(
      relBuilder: RelBuilder,
      oldAgg: Aggregate,
      newCalls: util.List[AggregateCall]): Unit = {

    // create a LogicalAggregate with simpler aggregation functions
    super.newAggregateRel(relBuilder, oldAgg, newCalls)
    // pop LogicalAggregate from RelBuilder
    val newAgg = relBuilder.build().asInstanceOf[LogicalAggregate]

    // create a new LogicalWindowAggregate (based on the new LogicalAggregate) and push it on the
    // RelBuilder
    val oldWindowAgg = oldAgg.asInstanceOf[LogicalWindowAggregate]
    val newWindowAgg = LogicalWindowAggregate.create(
      oldWindowAgg.getWindow,
      oldWindowAgg.getNamedProperties,
      newAgg)
    relBuilder.push(newWindowAgg)
  }

  override def newCalcRel(
      relBuilder: RelBuilder,
      rowType: RelDataType,
      exprs: util.List[RexNode]): Unit = {
    val numExprs = exprs.size()
    // add all named properties of the window to the selection
    rowType
      .getFieldList
      .subList(numExprs, rowType.getFieldCount).toList
      .foreach(f => exprs.add(relBuilder.field(f.getName)))
    // create a LogicalCalc that computes the complex aggregates and forwards the window properties
    relBuilder.project(exprs, rowType.getFieldNames)
  }

}

object WindowAggregateReduceFunctionsRule {
  val INSTANCE = new WindowAggregateReduceFunctionsRule
}
