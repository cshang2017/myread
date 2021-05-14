

package org.apache.flink.table.planner.plan.logical

import com.google.common.collect.ImmutableMap
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.{RelCollation, RelNode}
import org.apache.calcite.rex.RexNode
import org.apache.calcite.util.ImmutableBitSet

import java.util

/**
  * Describes MATCH RECOGNIZE clause.
  */
case class MatchRecognize(
  input: RelNode,
  rowType: RelDataType,
  pattern: RexNode,
  patternDefinitions: ImmutableMap[String, RexNode],
  measures: ImmutableMap[String, RexNode],
  after: RexNode,
  subsets: ImmutableMap[String, util.SortedSet[String]],
  allRows: Boolean,
  partitionKeys: ImmutableBitSet,
  orderKeys: RelCollation,
  interval: RexNode)
