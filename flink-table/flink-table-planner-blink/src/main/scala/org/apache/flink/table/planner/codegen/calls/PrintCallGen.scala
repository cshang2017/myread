

package org.apache.flink.table.planner.codegen.calls

import org.apache.flink.table.planner.codegen.CodeGenUtils.{newNames, primitiveTypeTermForType}
import org.apache.flink.table.planner.codegen.{CodeGeneratorContext, GeneratedExpression}
import org.apache.flink.table.runtime.typeutils.TypeCheckUtils.isBinaryString
import org.apache.flink.table.types.logical.LogicalType

import java.nio.charset.StandardCharsets

/**
  * Generates PRINT function call.
  */
class PrintCallGen extends CallGenerator {

  override def generate(
      ctx: CodeGeneratorContext,
      operands: Seq[GeneratedExpression],
      returnType: LogicalType): GeneratedExpression = {
    val Seq(resultTerm, nullTerm) = newNames("result", "isNull")
    val resultTypeTerm = primitiveTypeTermForType(returnType)

    // add logger, prefer name without number suffix to make sure only one definition
    // exists in each operator in case of multiple print(s)
    val logTerm = "logger$"
    ctx.addReusableLogger(logTerm, "_Print$_")

    val charsets = classOf[StandardCharsets].getCanonicalName
    val outputCode = if (isBinaryString(returnType)) {
      s"new String($resultTerm, $charsets.UTF_8)"
    } else {
      s"String.valueOf(${operands(1).resultTerm})"
    }

    val msgCode =
      s"""
         |(${operands.head.nullTerm} ? "null" : ${operands.head.resultTerm}.toString()) +
         |(${operands(1).nullTerm} ? "null" : $outputCode)
       """.stripMargin

    val resultCode =
      s"""
         |${operands(1).code};
         |$resultTypeTerm $resultTerm = ${operands(1).resultTerm};
         |boolean $nullTerm = ${operands(1).nullTerm};
         |org.slf4j.MDC.put("fromUser", "TRUE");
         |$logTerm.error($msgCode);
         |System.out.println($msgCode);
       """.stripMargin

    GeneratedExpression(resultTerm, nullTerm, resultCode, returnType)
  }
}
