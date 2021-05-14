package org.apache.flink.table.runtime

import org.apache.flink.api.common.functions.util.FunctionUtils
import org.apache.flink.api.common.functions.{MapFunction, RichMapFunction}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.ResultTypeQueryable
import org.apache.flink.configuration.Configuration
import org.apache.flink.table.codegen.Compiler
import org.apache.flink.table.util.Logging

class MapRunner[IN, OUT](
    name: String,
    code: String,
    @transient var returnType: TypeInformation[OUT])
  extends RichMapFunction[IN, OUT]
  with ResultTypeQueryable[OUT]
  with Compiler[MapFunction[IN, OUT]]
  with Logging {

  private var function: MapFunction[IN, OUT] = _

  override def open(parameters: Configuration): Unit = {
    val clazz = compile(getRuntimeContext.getUserCodeClassLoader, name, code)
    function = clazz.newInstance()
    FunctionUtils.setFunctionRuntimeContext(function, getRuntimeContext)
    FunctionUtils.openFunction(function, parameters)
  }

  override def map(in: IN): OUT =
    function.map(in)

  override def getProducedType: TypeInformation[OUT] = returnType

  override def close(): Unit = {
    FunctionUtils.closeFunction(function)
  }
}
