
package org.apache.flink.table.planner.plan.optimize.program

import org.apache.flink.configuration.Configuration
import org.apache.flink.table.api.config.OptimizerConfigOptions
import org.apache.flink.table.planner.plan.nodes.FlinkConventions
import org.apache.flink.table.planner.plan.rules.{FlinkBatchRuleSets, FlinkStreamRuleSets}

import org.apache.calcite.plan.hep.HepMatchOrder

/**
  * Defines a sequence of programs to optimize for stream table plan.
  */
object FlinkStreamProgram {

  val SUBQUERY_REWRITE = "subquery_rewrite"
  val TEMPORAL_JOIN_REWRITE = "temporal_join_rewrite"
  val DECORRELATE = "decorrelate"
  val TIME_INDICATOR = "time_indicator"
  val DEFAULT_REWRITE = "default_rewrite"
  val PREDICATE_PUSHDOWN = "predicate_pushdown"
  val JOIN_REORDER = "join_reorder"
  val LOGICAL = "logical"
  val LOGICAL_REWRITE = "logical_rewrite"
  val PHYSICAL = "physical"
  val PHYSICAL_REWRITE = "physical_rewrite"

  def buildProgram(config: Configuration): FlinkChainedProgram[StreamOptimizeContext] = {
    val chainedProgram = new FlinkChainedProgram[StreamOptimizeContext]()

    // rewrite sub-queries to joins
    chainedProgram.addLast(
      SUBQUERY_REWRITE,
      FlinkGroupProgramBuilder.newBuilder[StreamOptimizeContext]
        // rewrite QueryOperationCatalogViewTable before rewriting sub-queries
        .addProgram(FlinkHepRuleSetProgramBuilder.newBuilder
          .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_SEQUENCE)
          .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
          .add(FlinkStreamRuleSets.TABLE_REF_RULES)
          .build(), "convert table references before rewriting sub-queries to semi-join")
        .addProgram(FlinkHepRuleSetProgramBuilder.newBuilder
          .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_SEQUENCE)
          .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
          .add(FlinkStreamRuleSets.SEMI_JOIN_RULES)
          .build(), "rewrite sub-queries to semi-join")
        .addProgram(FlinkHepRuleSetProgramBuilder.newBuilder
          .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_COLLECTION)
          .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
          .add(FlinkStreamRuleSets.TABLE_SUBQUERY_RULES)
          .build(), "sub-queries remove")
        // convert RelOptTableImpl (which exists in SubQuery before) to FlinkRelOptTable
        .addProgram(FlinkHepRuleSetProgramBuilder.newBuilder
          .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_SEQUENCE)
          .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
          .add(FlinkStreamRuleSets.TABLE_REF_RULES)
          .build(), "convert table references after sub-queries removed")
        .build())

    // rewrite special temporal join plan
    chainedProgram.addLast(
      TEMPORAL_JOIN_REWRITE,
      FlinkGroupProgramBuilder.newBuilder[StreamOptimizeContext]
        .addProgram(
          FlinkHepRuleSetProgramBuilder.newBuilder
            .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_SEQUENCE)
            .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
            .add(FlinkStreamRuleSets.EXPAND_PLAN_RULES)
            .build(), "convert correlate to temporal table join")
        .addProgram(
          FlinkHepRuleSetProgramBuilder.newBuilder
            .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_SEQUENCE)
            .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
            .add(FlinkStreamRuleSets.POST_EXPAND_CLEAN_UP_RULES)
            .build(), "convert enumerable table scan")
        .build())

    // query decorrelation
    chainedProgram.addLast(DECORRELATE, new FlinkDecorrelateProgram)

    // convert time indicators
    chainedProgram.addLast(TIME_INDICATOR, new FlinkRelTimeIndicatorProgram)

    // default rewrite, includes: predicate simplification, expression reduction, window
    // properties rewrite, etc.
    chainedProgram.addLast(
      DEFAULT_REWRITE,
      FlinkHepRuleSetProgramBuilder.newBuilder
        .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_SEQUENCE)
        .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
        .add(FlinkStreamRuleSets.DEFAULT_REWRITE_RULES)
        .build())

    // rule based optimization: push down predicate(s) in where clause, so it only needs to read
    // the required data
    chainedProgram.addLast(
      PREDICATE_PUSHDOWN,
      FlinkGroupProgramBuilder.newBuilder[StreamOptimizeContext]
        .addProgram(
          FlinkHepRuleSetProgramBuilder.newBuilder
            .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_COLLECTION)
            .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
            .add(FlinkStreamRuleSets.FILTER_PREPARE_RULES)
            .build(), "filter rules")
        .addProgram(
          FlinkHepRuleSetProgramBuilder.newBuilder
            .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_SEQUENCE)
            .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
            .add(FlinkBatchRuleSets.FILTER_TABLESCAN_PUSHDOWN_RULES)
            .build(), "push predicate into table scan")
        .addProgram(
          FlinkHepRuleSetProgramBuilder.newBuilder
            .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_SEQUENCE)
            .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
            .add(FlinkStreamRuleSets.PRUNE_EMPTY_RULES)
            .build(), "prune empty after predicate push down")
        .build())

    // join reorder
    if (config.getBoolean(OptimizerConfigOptions.TABLE_OPTIMIZER_JOIN_REORDER_ENABLED)) {
      chainedProgram.addLast(
        JOIN_REORDER,
        FlinkGroupProgramBuilder.newBuilder[StreamOptimizeContext]
          .addProgram(FlinkHepRuleSetProgramBuilder.newBuilder
            .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_COLLECTION)
            .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
            .add(FlinkStreamRuleSets.JOIN_REORDER_PREPARE_RULES)
            .build(), "merge join into MultiJoin")
          .addProgram(FlinkHepRuleSetProgramBuilder.newBuilder
            .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_SEQUENCE)
            .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
            .add(FlinkStreamRuleSets.JOIN_REORDER_RULES)
            .build(), "do join reorder")
          .build())
    }

    // optimize the logical plan
    chainedProgram.addLast(
      LOGICAL,
      FlinkVolcanoProgramBuilder.newBuilder
        .add(FlinkStreamRuleSets.LOGICAL_OPT_RULES)
        .setRequiredOutputTraits(Array(FlinkConventions.LOGICAL))
        .build())

    // logical rewrite
    chainedProgram.addLast(
      LOGICAL_REWRITE,
      FlinkHepRuleSetProgramBuilder.newBuilder
        .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_SEQUENCE)
        .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
        .add(FlinkStreamRuleSets.LOGICAL_REWRITE)
        .build())

    // optimize the physical plan
    chainedProgram.addLast(
      PHYSICAL,
      FlinkVolcanoProgramBuilder.newBuilder
        .add(FlinkStreamRuleSets.PHYSICAL_OPT_RULES)
        .setRequiredOutputTraits(Array(FlinkConventions.STREAM_PHYSICAL))
        .build())

    // physical rewrite
    chainedProgram.addLast(
      PHYSICAL_REWRITE,
      FlinkGroupProgramBuilder.newBuilder[StreamOptimizeContext]
        .addProgram(new FlinkChangelogModeInferenceProgram,
          "Changelog mode inference")
        .addProgram(new FlinkMiniBatchIntervalTraitInitProgram,
          "Initialization for mini-batch interval inference")
        .addProgram(
          FlinkHepRuleSetProgramBuilder.newBuilder
            .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_SEQUENCE)
            .setHepMatchOrder(HepMatchOrder.TOP_DOWN)
            .add(FlinkStreamRuleSets.MINI_BATCH_RULES)
            .build(), "mini-batch interval rules")
        .addProgram(
          FlinkHepRuleSetProgramBuilder.newBuilder
            .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_COLLECTION)
            .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
            .add(FlinkStreamRuleSets.PHYSICAL_REWRITE)
            .build(), "physical rewrite")
        .build())

    chainedProgram
  }
}
