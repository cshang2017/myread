

package org.apache.flink.table.planner.calcite

import org.apache.flink.table.api.TableConfig
import org.apache.flink.table.catalog.{CatalogManager, FunctionCatalog}

import org.apache.calcite.plan.Context

/**
  * A [[Context]] to allow the store data within the planner session and access it within rules.
  */
trait FlinkContext extends Context {

  /**
    * Gets [[TableConfig]] instance defined in [[org.apache.flink.table.api.TableEnvironment]].
    */
  def getTableConfig: TableConfig

  /**
    * Gets [[FunctionCatalog]] instance defined in [[org.apache.flink.table.api.TableEnvironment]].
    */
  def getFunctionCatalog: FunctionCatalog

  /**
    * Gets [[CatalogManager]] instance defined in [[org.apache.flink.table.api.TableEnvironment]].
    */
  def getCatalogManager: CatalogManager

  /**
    * Gets [[SqlExprToRexConverterFactory]] instance to convert sql expression to rex node.
    */
  def getSqlExprToRexConverterFactory: SqlExprToRexConverterFactory

  override def unwrap[C](clazz: Class[C]): C = {
    if (clazz.isInstance(this)) clazz.cast(this) else null.asInstanceOf[C]
  }

}
