package org.apache.flink.table.planner.sinks

import org.apache.flink.annotation.Internal
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.table.api.{Table, TableException, TableSchema}
import org.apache.flink.table.sinks.TableSink

/**
  * A [[DataStreamTableSink]] specifies how to emit a [[Table]] to an DataStream[T]
  *
  * @param outputType The [[TypeInformation]] that specifies the type of the [[DataStream]].
  * @param needUpdateBefore Set to true to if need UPDATE_BEFORE messages when receiving updates
  * @param withChangeFlag Set to true to emit records with change flags.
  * @tparam T The type of the resulting [[DataStream]].
  */
@Internal
class DataStreamTableSink[T](
    tableSchema: TableSchema,
    outputType: TypeInformation[T],
    val needUpdateBefore: Boolean,
    val withChangeFlag: Boolean) extends TableSink[T] {

  /**
    * Return the type expected by this [[TableSink]].
    *
    * This type should depend on the types returned by [[getTableSchema]].
    *
    * @return The type expected by this [[TableSink]].
    */
  override def getOutputType: TypeInformation[T] = outputType

  override def getTableSchema: TableSchema = tableSchema

  override def configure(
      fieldNames: Array[String],
      fieldTypes: Array[TypeInformation[_]]): TableSink[T] = {
    throw new TableException(s"configure is not supported.")
  }

}
