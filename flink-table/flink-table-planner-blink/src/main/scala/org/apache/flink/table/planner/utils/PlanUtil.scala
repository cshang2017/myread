

package org.apache.flink.table.planner.utils

import org.apache.flink.streaming.api.graph.StreamGraph

import java.io.{PrintWriter, StringWriter}

import scala.collection.JavaConversions._


object PlanUtil extends Logging {

  /**
    * Converting an StreamGraph to a human-readable string.
    *
    * @param graph stream graph
    */
  def explainStreamGraph(graph: StreamGraph): String = {
    def isSource(id: Int): Boolean = graph.getSourceIDs.contains(id)

    def isSink(id: Int): Boolean = graph.getSinkIDs.contains(id)

    // can not convert to single abstract method because it will throw compile error
    implicit val order: Ordering[Int] = new Ordering[Int] {
      override def compare(x: Int, y: Int): Int = (isSink(x), isSink(y)) match {
        case (true, false) => 1
        case (false, true) => -1
        case (_, _) => x - y
      }
    }

    val operatorIDs = graph.getStreamNodes.map(_.getId).toList.sorted(order)
    val sw = new StringWriter()
    val pw = new PrintWriter(sw)

    var tabs = 0
    operatorIDs.foreach { id =>
      val op = graph.getStreamNode(id)
      val (nodeType, content) = if (isSource(id)) {
        tabs = 0
        ("Data Source", op.getOperatorName)
      } else if (isSink(id)) {
        ("Data Sink", op.getOperatorName)
      } else {
        ("Operator", op.getOperatorName)
      }

      pw.append("\t" * tabs).append(s"Stage $id : $nodeType\n")
        .append("\t" * (tabs + 1)).append(s"content : $content\n")

      if (!isSource(id)) {
        val partition = op.getInEdges.head.getPartitioner.toString
        pw.append("\t" * (tabs + 1)).append(s"ship_strategy : $partition\n")
      }

      pw.append("\n")
      tabs += 1
    }

    pw.close()
    sw.toString
  }

}
