
package org.apache.flink.table

package object codegen {
  // Used in ExpressionCodeGenerator because Scala 2.10 reflection is not thread safe. We might
  // have several parallel expression operators in one TaskManager, therefore we need to guard
  // these operations.
  object ReflectionLock
}
