package org.apache.flink.table.planner.calcite

import org.apache.flink.table.api.TableConfig
import org.apache.flink.table.catalog.{CatalogManager, FunctionCatalog}

class FlinkContextImpl(
    tableConfig: TableConfig,
    functionCatalog: FunctionCatalog,
    catalogManager: CatalogManager,
    toRexFactory: SqlExprToRexConverterFactory)
  extends FlinkContext {

  override def getTableConfig: TableConfig = tableConfig

  override def getFunctionCatalog: FunctionCatalog = functionCatalog

  override def getCatalogManager: CatalogManager = catalogManager

  override def getSqlExprToRexConverterFactory: SqlExprToRexConverterFactory = toRexFactory
}
