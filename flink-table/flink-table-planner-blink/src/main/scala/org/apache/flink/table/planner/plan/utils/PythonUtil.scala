

package org.apache.flink.table.planner.plan.utils

import org.apache.calcite.rex.{RexCall, RexNode}
import org.apache.flink.table.functions.FunctionDefinition
import org.apache.flink.table.functions.python.{PythonFunction, PythonFunctionKind}
import org.apache.flink.table.planner.functions.bridging.BridgingSqlFunction
import org.apache.flink.table.planner.functions.utils.{ScalarSqlFunction, TableSqlFunction}

import scala.collection.JavaConversions._

object PythonUtil {

  /**
    * Checks whether it contains the specified kind of Python function call in the specified node.
    * If the parameter pythonFunctionKind is null, it will return true for any kind of Python
    * function.
    *
    * @param node the RexNode to check
    * @param pythonFunctionKind the kind of the python function
    * @return true if it contains the Python function call in the specified node.
    */
  def containsPythonCall(node: RexNode, pythonFunctionKind: PythonFunctionKind = null): Boolean =
    node.accept(new FunctionFinder(true, Option(pythonFunctionKind), true))

  /**
    * Checks whether it contains non-Python function call in the specified node.
    *
    * @param node the RexNode to check
    * @return true if it contains the non-Python function call in the specified node.
    */
  def containsNonPythonCall(node: RexNode): Boolean =
    node.accept(new FunctionFinder(false, None, true))

  /**
    * Checks whether the specified node is the specified kind of Python function call.
    * If the parameter pythonFunctionKind is null, it will return true for any kind of Python
    * function.
    *
    * @param node the RexNode to check
    * @param pythonFunctionKind the kind of the python function
    * @return true if the specified node is a Python function call.
    */
  def isPythonCall(node: RexNode, pythonFunctionKind: PythonFunctionKind = null): Boolean =
    node.accept(new FunctionFinder(true, Option(pythonFunctionKind), false))

  /**
    * Checks whether the specified node is a non-Python function call.
    *
    * @param node the RexNode to check
    * @return true if the specified node is a non-Python function call.
    */
  def isNonPythonCall(node: RexNode): Boolean = node.accept(new FunctionFinder(false, None, false))

  /**
    * Checks whether it contains the specified kind of function in a RexNode.
    *
    * @param findPythonFunction true to find python function, false to find non-python function
    * @param pythonFunctionKind the kind of the python function
    * @param recursive whether check the inputs
    */
  private class FunctionFinder(
      findPythonFunction: Boolean,
      pythonFunctionKind: Option[PythonFunctionKind],
      recursive: Boolean)
    extends RexDefaultVisitor[Boolean] {

    /**
      * Checks whether the specified rexCall is a python function call of the specified kind.
      *
      * @param rexCall the RexCall to check.
      * @return true if it is python function call of the specified kind.
      */
    private def isPythonRexCall(rexCall: RexCall): Boolean =
      rexCall.getOperator match {
        case sfc: ScalarSqlFunction => isPythonFunction(sfc.scalarFunction)
        case tfc: TableSqlFunction => isPythonFunction(tfc.udtf)
        case bsf: BridgingSqlFunction => isPythonFunction(bsf.getDefinition)
        case _ => false
    }

    private def isPythonFunction(functionDefinition: FunctionDefinition): Boolean = {
      functionDefinition.isInstanceOf[PythonFunction] &&
        (pythonFunctionKind.isEmpty ||
          functionDefinition.asInstanceOf[PythonFunction].getPythonFunctionKind ==
            pythonFunctionKind.get)
    }

    override def visitCall(call: RexCall): Boolean = {
      findPythonFunction == isPythonRexCall(call) ||
        (recursive && call.getOperands.exists(_.accept(this)))
    }

    override def visitNode(rexNode: RexNode): Boolean = false
  }
}
