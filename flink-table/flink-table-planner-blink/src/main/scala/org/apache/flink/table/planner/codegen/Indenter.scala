
package org.apache.flink.table.planner.codegen

class IndentStringContext(sc: StringContext) {
  def j(args: Any*): String = {
    val sb = new StringBuilder()
    for ((s, a) <- sc.parts zip args) {
      sb append s

      val ind = getindent(s)
      if (ind.nonEmpty) {
        sb append a.toString.replaceAll("\n", "\n" + ind)
      } else {
        sb append a.toString
      }
    }
    if (sc.parts.size > args.size) {
      sb append sc.parts.last
    }

    sb.toString()
  }

  // get white indent after the last new line, if any
  def getindent(str: String): String = {
    val lastnl = str.lastIndexOf("\n")
    if (lastnl == -1) ""
    else {
      val ind = str.substring(lastnl + 1)
      val trimmed = ind.trim
      if (trimmed.isEmpty || trimmed == "|") {
        ind // ind is all whitespace or pipe for use with stripMargin. Use this
      } else {
        ""
      }
    }
  }
}

object Indenter {
  implicit def toISC(sc: StringContext): IndentStringContext = new IndentStringContext(sc)
}
