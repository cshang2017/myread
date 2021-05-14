package org.apache.flink.table.planner.codegen.sort

import org.apache.flink.table.api.TableConfig
import org.apache.flink.table.planner.codegen.CodeGenUtils.{ROW_DATA, newName}
import org.apache.flink.table.planner.codegen.Indenter.toISC
import org.apache.flink.table.planner.codegen.{CodeGeneratorContext, GenerateUtils}
import org.apache.flink.table.runtime.generated.{GeneratedRecordComparator, RecordComparator}
import org.apache.flink.table.types.logical.LogicalType

/**
  * A code generator for generating [[RecordComparator]].
  */
object ComparatorCodeGenerator {

  /**
    * Generates a [[RecordComparator]] that can be passed to a Java compiler.
    *
    * @param conf        Table config.
    * @param name        Class name of the function.
    *                    Does not need to be unique but has to be a valid Java class identifier.
    * @param keys        key positions describe which fields are keys in what order.
    * @param keyTypes    types for the key fields, in the same order as the key fields.
    * @param orders      sorting orders for the key fields.
    * @param nullsIsLast Ordering of nulls.
    * @return A GeneratedRecordComparator
    */
  def gen(
      conf: TableConfig,
      name: String,
      keys: Array[Int],
      keyTypes: Array[LogicalType],
      orders: Array[Boolean],
      nullsIsLast: Array[Boolean]): GeneratedRecordComparator = {
    val className = newName(name)
    val baseClass = classOf[RecordComparator]

    val ctx = new CodeGeneratorContext(conf)
    val compareCode = GenerateUtils.generateRowCompare(
      ctx, keys, keyTypes, orders, nullsIsLast, "o1", "o2")

    val code =
      j"""
      public class $className implements ${baseClass.getCanonicalName} {

        private final Object[] references;
        ${ctx.reuseMemberCode()}

        public $className(Object[] references) {
          this.references = references;
          ${ctx.reuseInitCode()}
          ${ctx.reuseOpenCode()}
        }

        @Override
        public int compare($ROW_DATA o1, $ROW_DATA o2) {
          $compareCode
          return 0;
        }

      }
      """.stripMargin

    new GeneratedRecordComparator(className, code, ctx.references.toArray)
  }

}
