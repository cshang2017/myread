package org.apache.flink.table.planner.plan.schema

import org.apache.flink.table.functions
import org.apache.flink.table.types.DataType

import org.apache.calcite.rel.`type`.{RelDataType, RelDataTypeFactory}
import org.apache.calcite.schema.{FunctionParameter, TableFunction}

import java.lang.reflect.Type
import java.util
import java.util.Collections

/**
  * A [[FlinkTableFunction]] is an implementation of  [[org.apache.calcite.schema.TableFunction]].
  * It is also an abstraction and instance holder of Table Function in Flink's Table API & SQL.
  * We can create different kinds of [[FlinkTableFunction]] for different usages.
  *
  * @param tableFunction The Table Function instance
  */
abstract class FlinkTableFunction(
    tableFunction: functions.TableFunction[_])
  extends TableFunction {

  override def getElementType(arguments: util.List[AnyRef]): Type = classOf[Array[Object]]

  // we do never use the FunctionParameters, so return an empty list
  override def getParameters: util.List[FunctionParameter] = Collections.emptyList()
  // we do never use the getRowType, so return null
  override def getRowType(
      typeFactory: RelDataTypeFactory,
      arguments: util.List[AnyRef]): RelDataType = null

  /**
    * Returns the Type for usage, i.e. code generation.
    */
  def getExternalResultType(
      tableFunction: functions.TableFunction[_],
      arguments: Array[AnyRef],
      argTypes: Array[Class[_]]): DataType

  def getRowType(typeFactory: RelDataTypeFactory): RelDataType
}
