package org.apache.flink.table.planner.plan.optimize.program

import org.apache.flink.table.planner.calcite.RelTimeIndicatorConverter
import org.apache.flink.util.Preconditions

import org.apache.calcite.rel.RelNode

/**
  * A FlinkOptimizeProgram that deals with time.
  */
class FlinkRelTimeIndicatorProgram extends FlinkOptimizeProgram[StreamOptimizeContext] {

  override def optimize(input: RelNode, context: StreamOptimizeContext): RelNode = {
    val rexBuilder = Preconditions.checkNotNull(context.getRexBuilder)
    RelTimeIndicatorConverter.convert(input, rexBuilder, context.needFinalTimeIndicatorConversion)
  }

}
