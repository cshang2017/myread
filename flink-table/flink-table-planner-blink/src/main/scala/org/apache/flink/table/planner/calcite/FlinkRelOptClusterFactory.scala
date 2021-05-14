

package org.apache.flink.table.planner.calcite

import org.apache.flink.table.planner.plan.metadata.{FlinkDefaultRelMetadataProvider, FlinkRelMetadataQuery}

import org.apache.calcite.plan.{RelOptCluster, RelOptPlanner}
import org.apache.calcite.rel.metadata.{DefaultRelMetadataProvider, RelMetadataQuery}
import org.apache.calcite.rex.RexBuilder

import java.util.function.Supplier

/**
  * The utility class is to create special [[RelOptCluster]] instance which use
  * [[FlinkDefaultRelMetadataProvider]] instead of [[DefaultRelMetadataProvider]].
  */
object FlinkRelOptClusterFactory {

  def create(planner: RelOptPlanner, rexBuilder: RexBuilder): RelOptCluster = {
    val cluster = RelOptCluster.create(planner, rexBuilder)
    cluster.setMetadataProvider(FlinkDefaultRelMetadataProvider.INSTANCE)
    cluster.setMetadataQuerySupplier(new Supplier[RelMetadataQuery]() {
      def get: FlinkRelMetadataQuery = FlinkRelMetadataQuery.instance()
    })
    cluster
  }

}
