package org.apache.flink.table.planner.plan.optimize.program

import org.apache.flink.configuration.Configuration
import org.apache.flink.table.api.config.OptimizerConfigOptions
import org.apache.flink.table.planner.plan.nodes.FlinkConventions
import org.apache.flink.table.planner.plan.rules.FlinkBatchRuleSets

import org.apache.calcite.plan.hep.HepMatchOrder

/**
  * Defines a sequence of programs to optimize flink batch table plan.
  */
object FlinkBatchProgram {
  val SUBQUERY_REWRITE = "subquery_rewrite"
  val TEMPORAL_JOIN_REWRITE = "temporal_join_rewrite"
  val DECORRELATE = "decorrelate"
  val DEFAULT_REWRITE = "default_rewrite"
  val PREDICATE_PUSHDOWN = "predicate_pushdown"
  val JOIN_REORDER = "join_reorder"
  val JOIN_REWRITE = "join_rewrite"
  val WINDOW = "window"
  val LOGICAL = "logical"
  val LOGICAL_REWRITE = "logical_rewrite"
  val PHYSICAL = "physical"
  val PHYSICAL_REWRITE = "physical_rewrite"

  def buildProgram(config: Configuration): FlinkChainedProgram[BatchOptimizeContext] = {
    val chainedProgram = new FlinkChainedProgram[BatchOptimizeContext]()

    chainedProgram.addLast(
       // rewrite sub-queries to joins
      SUBQUERY_REWRITE,
      FlinkGroupProgramBuilder.newBuilder[BatchOptimizeContext]
        // rewrite QueryOperationCatalogViewTable before rewriting sub-queries
        .addProgram(FlinkHepRuleSetProgramBuilder.newBuilder
          .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_SEQUENCE)
          .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
          .add(FlinkBatchRuleSets.TABLE_REF_RULES)
          .build(), "convert table references before rewriting sub-queries to semi-join")
        .addProgram(FlinkHepRuleSetProgramBuilder.newBuilder
          .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_SEQUENCE)
          .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
          .add(FlinkBatchRuleSets.SEMI_JOIN_RULES)
          .build(), "rewrite sub-queries to semi-join")
        .addProgram(FlinkHepRuleSetProgramBuilder.newBuilder
          .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_COLLECTION)
          .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
          .add(FlinkBatchRuleSets.TABLE_SUBQUERY_RULES)
          .build(), "sub-queries remove")
        // convert RelOptTableImpl (which exists in SubQuery before) to FlinkRelOptTable
        .addProgram(FlinkHepRuleSetProgramBuilder.newBuilder
          .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_SEQUENCE)
          .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
          .add(FlinkBatchRuleSets.TABLE_REF_RULES)
          .build(), "convert table references after sub-queries removed")
        .build()
    )

    // rewrite special temporal join plan
    chainedProgram.addLast(
      TEMPORAL_JOIN_REWRITE,
      FlinkGroupProgramBuilder.newBuilder[BatchOptimizeContext]
        .addProgram(
          FlinkHepRuleSetProgramBuilder.newBuilder
            .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_SEQUENCE)
            .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
            .add(FlinkBatchRuleSets.EXPAND_PLAN_RULES)
            .build(), "convert correlate to temporal table join")
        .addProgram(
          FlinkHepRuleSetProgramBuilder.newBuilder
            .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_SEQUENCE)
            .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
            .add(FlinkBatchRuleSets.POST_EXPAND_CLEAN_UP_RULES)
            .build(), "convert enumerable table scan")
        .build())

    // query decorrelation
    chainedProgram.addLast(DECORRELATE, new FlinkDecorrelateProgram)

    // default rewrite, includes: predicate simplification, expression reduction, etc.
    chainedProgram.addLast(
      DEFAULT_REWRITE,
      FlinkHepRuleSetProgramBuilder.newBuilder
        .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_SEQUENCE)
        .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
        .add(FlinkBatchRuleSets.DEFAULT_REWRITE_RULES)
        .build())

    // rule based optimization: push down predicate(s)
    chainedProgram.addLast(
      PREDICATE_PUSHDOWN,
      FlinkGroupProgramBuilder.newBuilder[BatchOptimizeContext]
        .addProgram(
          FlinkGroupProgramBuilder.newBuilder[BatchOptimizeContext]
            .addProgram(
              FlinkHepRuleSetProgramBuilder.newBuilder
                .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_SEQUENCE)
                .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
                .add(FlinkBatchRuleSets.JOIN_PREDICATE_REWRITE_RULES)
                .build(), "join predicate rewrite")
            .addProgram(
              FlinkHepRuleSetProgramBuilder.newBuilder
                .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_COLLECTION)
                .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
                .add(FlinkBatchRuleSets.FILTER_PREPARE_RULES)
                .build(), "other predicate rewrite")
            .setIterations(5).build(), "predicate rewrite")
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
            .add(FlinkBatchRuleSets.PRUNE_EMPTY_RULES)
            .build(), "prune empty after predicate push down")
        .build())

    // join reorder
    if (config.getBoolean(OptimizerConfigOptions.TABLE_OPTIMIZER_JOIN_REORDER_ENABLED)) {
      chainedProgram.addLast(
        JOIN_REORDER,
        FlinkGroupProgramBuilder.newBuilder[BatchOptimizeContext]
          .addProgram(FlinkHepRuleSetProgramBuilder.newBuilder
            .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_COLLECTION)
            .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
            .add(FlinkBatchRuleSets.JOIN_REORDER_PREPARE_RULES)
            .build(), "merge join into MultiJoin")
          .addProgram(FlinkHepRuleSetProgramBuilder.newBuilder
            .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_SEQUENCE)
            .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
            .add(FlinkBatchRuleSets.JOIN_REORDER_RULES)
            .build(), "do join reorder")
          .build())
    }

    // join rewrite
    chainedProgram.addLast(
      JOIN_REWRITE,
      FlinkHepRuleSetProgramBuilder.newBuilder
        .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_COLLECTION)
        .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
        .add(FlinkBatchRuleSets.JOIN_COND_EQUAL_TRANSFER_RULES)
        .build())

    // window rewrite
    chainedProgram.addLast(
      WINDOW,
      FlinkGroupProgramBuilder.newBuilder[BatchOptimizeContext]
        .addProgram(
          FlinkHepRuleSetProgramBuilder.newBuilder
            .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_COLLECTION)
            .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
            .add(FlinkBatchRuleSets.PROJECT_RULES)
            .build(), "project rules")
        .addProgram(
          FlinkHepRuleSetProgramBuilder.newBuilder
            .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_SEQUENCE)
            .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
            .add(FlinkBatchRuleSets.WINDOW_RULES)
            .build(), "window")
        .build())

    // optimize the logical plan
    chainedProgram.addLast(
      LOGICAL,
      FlinkVolcanoProgramBuilder.newBuilder
        .add(FlinkBatchRuleSets.LOGICAL_OPT_RULES)
        .setRequiredOutputTraits(Array(FlinkConventions.LOGICAL))
        .build())

    // logical rewrite
    chainedProgram.addLast(
      LOGICAL_REWRITE,
      FlinkHepRuleSetProgramBuilder.newBuilder
        .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_SEQUENCE)
        .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
        .add(FlinkBatchRuleSets.LOGICAL_REWRITE)
        .build())

    // optimize the physical plan
    chainedProgram.addLast(
      PHYSICAL,
      FlinkVolcanoProgramBuilder.newBuilder
        .add(FlinkBatchRuleSets.PHYSICAL_OPT_RULES)
        .setRequiredOutputTraits(Array(FlinkConventions.BATCH_PHYSICAL))
        .build())

    // physical rewrite
    chainedProgram.addLast(
      PHYSICAL_REWRITE,
      FlinkHepRuleSetProgramBuilder.newBuilder
        .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_SEQUENCE)
        .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
        .add(FlinkBatchRuleSets.PHYSICAL_REWRITE)
        .build())

    chainedProgram
  }
}
