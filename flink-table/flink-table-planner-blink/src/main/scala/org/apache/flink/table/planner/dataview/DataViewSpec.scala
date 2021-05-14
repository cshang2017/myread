package org.apache.flink.table.planner.dataview

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.table.dataview.{ListViewTypeInfo, MapViewTypeInfo}

/**
  * Data view specification.
  */
trait DataViewSpec {
  def stateId: String

  def fieldIndex: Int

  def dataViewTypeInfo: TypeInformation[_]
}

case class ListViewSpec[T](
    stateId: String,
    fieldIndex: Int,
    dataViewTypeInfo: ListViewTypeInfo[T])
  extends DataViewSpec {
}


case class MapViewSpec[K, V](
    stateId: String,
    fieldIndex: Int,
    dataViewTypeInfo: MapViewTypeInfo[K, V])
  extends DataViewSpec {
}
