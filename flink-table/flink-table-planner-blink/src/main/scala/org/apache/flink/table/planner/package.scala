package org.apache.flink.table

package object planner {

  type JBoolean = java.lang.Boolean
  type JByte = java.lang.Byte
  type JShort = java.lang.Short
  type JFloat = java.lang.Float
  type JInt = java.lang.Integer
  type JLong = java.lang.Long
  type JDouble = java.lang.Double
  type JString = java.lang.String
  type JBigDecimal = java.math.BigDecimal

  type JList[E] = java.util.List[E]
  type JArrayList[E] = java.util.ArrayList[E]

  type JMap[K, V] = java.util.Map[K, V]
  type JHashMap[K, V] = java.util.HashMap[K, V]

  type JSet[E] = java.util.Set[E]
  type JHashSet[E] = java.util.HashSet[E]

  type CalcitePair[T, R] = org.apache.calcite.util.Pair[T, R]

}
