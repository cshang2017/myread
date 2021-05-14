package org.apache.flink.table.planner.plan.optimize.program

import org.apache.flink.table.api.TableException
import org.apache.flink.table.planner.plan.metadata.FlinkRelMdNonCumulativeCost
import org.apache.flink.util.Preconditions

import org.apache.calcite.plan.RelTrait
import org.apache.calcite.plan.hep.{HepPlanner, HepProgram}
import org.apache.calcite.rel.RelNode

/**
  * A FlinkOptimizeProgram that runs with [[HepPlanner]].
  *
  * <p>In most case, [[FlinkHepRuleSetProgram]] could meet our requirements.
  * Otherwise we could choose this program for some advanced features,
  * and use [[org.apache.calcite.plan.hep.HepProgramBuilder]] to create [[HepProgram]].
  *
  * @tparam OC OptimizeContext
  */
class FlinkHepProgram[OC <: FlinkOptimizeContext] extends FlinkOptimizeProgram[OC] {

  /**
    * [[HepProgram]] instance for [[HepPlanner]],
    * this must not be None when doing optimize.
    */
  private var hepProgram: Option[HepProgram] = None

  /**
    * Requested root traits, it's an optional item.
    */
  private var requestedRootTraits: Option[Array[RelTrait]] = None

  override def optimize(root: RelNode, context: OC): RelNode = {
    if (hepProgram.isEmpty) {
      throw new TableException("hepProgram should not be None in FlinkHepProgram")
    }

    try {
      val planner = new HepPlanner(hepProgram.get, context)
      FlinkRelMdNonCumulativeCost.THREAD_PLANNER.set(planner)

      planner.setRoot(root)

      if (requestedRootTraits.isDefined) {
        val targetTraitSet = root.getTraitSet.plusAll(requestedRootTraits.get)
        if (!root.getTraitSet.equals(targetTraitSet)) {
          planner.changeTraits(root, targetTraitSet.simplify)
        }
      }

      planner.findBestExp
    }  finally {
      FlinkRelMdNonCumulativeCost.THREAD_PLANNER.remove()
    }
  }

  /**
    * Sets hep program instance.
    */
  def setHepProgram(hepProgram: HepProgram): Unit = {
    Preconditions.checkNotNull(hepProgram)
    this.hepProgram = Some(hepProgram)
  }

  /**
    * Sets requested root traits.
    */
  def setRequestedRootTraits(relTraits: Array[RelTrait]): Unit = {
    requestedRootTraits = Option.apply(relTraits)
  }

}

object FlinkHepProgram {

  def apply[OC <: FlinkOptimizeContext](
      hepProgram: HepProgram,
      requestedRootTraits: Option[Array[RelTrait]] = None): FlinkHepProgram[OC] = {

    val flinkHepProgram = new FlinkHepProgram[OC]()
    flinkHepProgram.setHepProgram(hepProgram)
    if (requestedRootTraits.isDefined) {
      flinkHepProgram.setRequestedRootTraits(requestedRootTraits.get)
    }
    flinkHepProgram
  }
}
