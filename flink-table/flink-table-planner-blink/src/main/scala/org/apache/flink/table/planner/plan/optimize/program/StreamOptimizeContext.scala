package org.apache.flink.table.planner.plan.optimize.program

import org.apache.flink.table.planner.plan.`trait`.MiniBatchInterval

import org.apache.calcite.rex.RexBuilder

/**
  * A OptimizeContext allows to obtain stream table environment information when optimizing.
  */
trait StreamOptimizeContext extends FlinkOptimizeContext {

  /**
    * Gets the Calcite [[RexBuilder]] defined in [[org.apache.flink.table.api.TableEnvironment]].
    */
  def getRexBuilder: RexBuilder

  /**
   * Returns true if the root is required to send UPDATE_BEFORE message with
   * UPDATE_AFTER message together for update changes.
   */
  def isUpdateBeforeRequired: Boolean

  /**
    * Returns the mini-batch interval that sink requests.
    */
  def getMiniBatchInterval: MiniBatchInterval

  /**
    * Returns true if the output node needs final TimeIndicator conversion
    * defined in [[org.apache.flink.table.api.TableEnvironment#optimize]].
    */
  def needFinalTimeIndicatorConversion: Boolean

}
