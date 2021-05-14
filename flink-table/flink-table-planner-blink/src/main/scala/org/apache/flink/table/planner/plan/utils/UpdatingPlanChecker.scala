
package org.apache.flink.table.planner.plan.utils

import org.apache.flink.table.planner.delegation.PlannerBase
import org.apache.flink.table.planner.plan.metadata.FlinkRelMetadataQuery
import org.apache.flink.table.planner.plan.nodes.calcite.LegacySink
import org.apache.flink.table.sinks.UpsertStreamTableSink

import scala.collection.JavaConversions._

object UpdatingPlanChecker {

  def getUniqueKeyForUpsertSink(
      sinkNode: LegacySink,
      planner: PlannerBase,
      sink: UpsertStreamTableSink[_]): Option[Array[String]] = {
    // extract unique key fields
    // Now we pick shortest one to sink
    // TODO UpsertStreamTableSink setKeyFields interface should be Array[Array[String]]
    val sinkFieldNames = sink.getTableSchema.getFieldNames
    /** Extracts the unique keys of the table produced by the plan. */
    val fmq = FlinkRelMetadataQuery.reuseOrCreate(
      planner.getRelBuilder.getCluster.getMetadataQuery)
    val uniqueKeys = fmq.getUniqueKeys(sinkNode.getInput)
    if (uniqueKeys != null && uniqueKeys.size() > 0) {
      uniqueKeys
          .filter(_.nonEmpty)
          .map(_.toArray.map(sinkFieldNames))
          .toSeq
          .sortBy(_.length)
          .headOption
    } else {
      None
    }
  }
}
