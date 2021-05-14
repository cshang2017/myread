package org.apache.flink.table.planner.plan.metadata

import com.google.common.collect.ImmutableList
import org.apache.calcite.rel.metadata._

object FlinkDefaultRelMetadataProvider {

  val INSTANCE: RelMetadataProvider = ChainedRelMetadataProvider.of(
    ImmutableList.of(
      FlinkRelMdPercentageOriginalRows.SOURCE,
      FlinkRelMdNonCumulativeCost.SOURCE,
      FlinkRelMdCumulativeCost.SOURCE,
      FlinkRelMdRowCount.SOURCE,
      FlinkRelMdSize.SOURCE,
      FlinkRelMdSelectivity.SOURCE,
      FlinkRelMdDistinctRowCount.SOURCE,
      FlinkRelMdColumnInterval.SOURCE,
      FlinkRelMdFilteredColumnInterval.SOURCE,
      FlinkRelMdDistribution.SOURCE,
      FlinkRelMdColumnNullCount.SOURCE,
      FlinkRelMdColumnOriginNullCount.SOURCE,
      FlinkRelMdPopulationSize.SOURCE,
      FlinkRelMdColumnUniqueness.SOURCE,
      FlinkRelMdUniqueKeys.SOURCE,
      FlinkRelMdUniqueGroups.SOURCE,
      FlinkRelMdModifiedMonotonicity.SOURCE,
      RelMdColumnOrigins.SOURCE,
      RelMdMaxRowCount.SOURCE,
      RelMdMinRowCount.SOURCE,
      RelMdPredicates.SOURCE,
      FlinkRelMdCollation.SOURCE,
      RelMdExplainVisibility.SOURCE
    )
  )
}
