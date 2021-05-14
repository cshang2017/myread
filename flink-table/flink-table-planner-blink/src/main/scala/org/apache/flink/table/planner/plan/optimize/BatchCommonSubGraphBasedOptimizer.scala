

package org.apache.flink.table.planner.plan.optimize

import org.apache.flink.table.api.TableConfig
import org.apache.flink.table.catalog.{CatalogManager, FunctionCatalog}
import org.apache.flink.table.planner.calcite.{FlinkContext, SqlExprToRexConverterFactory}
import org.apache.flink.table.planner.delegation.BatchPlanner
import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchExecLegacySink
import org.apache.flink.table.planner.plan.optimize.program.{BatchOptimizeContext, FlinkBatchProgram}
import org.apache.flink.table.planner.plan.schema.IntermediateRelTable
import org.apache.flink.table.planner.utils.TableConfigUtils
import org.apache.flink.util.Preconditions

import org.apache.calcite.rel.RelNode

import java.util.Collections

/**
  * A [[CommonSubGraphBasedOptimizer]] for Batch.
  */
class BatchCommonSubGraphBasedOptimizer(planner: BatchPlanner)
  extends CommonSubGraphBasedOptimizer {

  override protected def doOptimize(roots: Seq[RelNode]): Seq[RelNodeBlock] = {
    // build RelNodeBlock plan
    val rootBlocks = RelNodeBlockPlanBuilder.buildRelNodeBlockPlan(roots, planner.getTableConfig)
    // optimize recursively RelNodeBlock
    rootBlocks.foreach(optimizeBlock)
    rootBlocks
  }

  private def optimizeBlock(block: RelNodeBlock): Unit = {
    block.children.foreach { child =>
      if (child.getNewOutputNode.isEmpty) {
        optimizeBlock(child)
      }
    }

    val originTree = block.getPlan
    val optimizedTree = optimizeTree(originTree)

    optimizedTree match {
      case _: BatchExecLegacySink[_] => // ignore
      case _ =>
        val name = createUniqueIntermediateRelTableName
        val intermediateRelTable =  new IntermediateRelTable(Collections.singletonList(name),
          optimizedTree)
        val newTableScan = wrapIntermediateRelTableToTableScan(intermediateRelTable, name)
        block.setNewOutputNode(newTableScan)
        block.setOutputTableName(name)
    }
    block.setOptimizedPlan(optimizedTree)
  }

  /**
    * Generates the optimized [[RelNode]] tree from the original relational node tree.
    *
    * @param relNode The original [[RelNode]] tree
    * @return The optimized [[RelNode]] tree
    */
  private def optimizeTree(relNode: RelNode): RelNode = {
    val config = planner.getTableConfig
    val programs = TableConfigUtils.getCalciteConfig(config).getBatchProgram
      .getOrElse(FlinkBatchProgram.buildProgram(config.getConfiguration))
    Preconditions.checkNotNull(programs)

    val context = relNode.getCluster.getPlanner.getContext.unwrap(classOf[FlinkContext])

    programs.optimize(relNode, new BatchOptimizeContext {
      override def getTableConfig: TableConfig = config

      override def getFunctionCatalog: FunctionCatalog = planner.functionCatalog

      override def getCatalogManager: CatalogManager = planner.catalogManager

      override def getSqlExprToRexConverterFactory: SqlExprToRexConverterFactory =
        context.getSqlExprToRexConverterFactory
    })
  }

}
