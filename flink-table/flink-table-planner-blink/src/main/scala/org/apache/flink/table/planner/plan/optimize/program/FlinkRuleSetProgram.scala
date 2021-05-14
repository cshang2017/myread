package org.apache.flink.table.planner.plan.optimize.program

import org.apache.flink.util.Preconditions

import org.apache.calcite.plan.RelOptRule
import org.apache.calcite.tools.RuleSet

import java.util

import scala.collection.JavaConversions._

/**
  * A FlinkOptimizeProgram that transforms a relational expression into
  * another relational expression with [[RuleSet]].
  */
abstract class FlinkRuleSetProgram[OC <: FlinkOptimizeContext] extends FlinkOptimizeProgram[OC] {

  /**
    * All [[RelOptRule]]s for optimizing associated with this program.
    */
  protected val rules: util.List[RelOptRule] = new util.ArrayList[RelOptRule]()

  /**
    * Adds specified rules to this program.
    */
  def add(ruleSet: RuleSet): Unit = {
    Preconditions.checkNotNull(ruleSet)
    ruleSet.foreach { rule =>
      if (!contains(rule)) {
        rules.add(rule)
      }
    }
  }

  /**
    * Removes specified rules from this program.
    */
  def remove(ruleSet: RuleSet): Unit = {
    Preconditions.checkNotNull(ruleSet)
    ruleSet.foreach(rules.remove)
  }

  /**
    * Removes all rules from this program first, and then adds specified rules to this program.
    */
  def replaceAll(ruleSet: RuleSet): Unit = {
    Preconditions.checkNotNull(ruleSet)
    rules.clear()
    ruleSet.foreach(rules.add)
  }

  /**
    * Checks whether this program contains the specified rule.
    */
  def contains(rule: RelOptRule): Boolean = {
    Preconditions.checkNotNull(rule)
    rules.contains(rule)
  }
}
