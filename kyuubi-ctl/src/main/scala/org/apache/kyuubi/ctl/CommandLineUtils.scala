/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kyuubi.ctl

import java.io.PrintStream

import org.apache.commons.lang3.StringUtils

/**
 * Contains basic command line parsing functionality and methods to parse some common Kyuubi Ctl
 * options.
 */
private[kyuubi] trait CommandLineUtils extends CommandLineLoggingUtils {

  def main(args: Array[String]): Unit
}

private[kyuubi] trait CommandLineLoggingUtils {
  // Exposed for testing
  private[kyuubi] var exitFn: Int => Unit = (exitCode: Int) => System.exit(exitCode)

  private[kyuubi] var printStream: PrintStream = System.err

  // scalastyle:off println
  private[kyuubi] def printMessage(msg: Any): Unit = printStream.println(msg)
  // scalastyle:on println
}

/** Refer the showString method of org.apache.spark.sql.Dataset */
private[kyuubi] object Tabulator {

  /**
   * Regular expression matching full width characters.
   *
   * Looked at all the 0x0000-0xFFFF characters (unicode) and showed them under Xshell.
   * Found all the full width characters, then get the regular expression.
   */
  private val fullWidthRegex = ("""[""" +
    // scalastyle:off nonascii
    """\u1100-\u115F""" +
    """\u2E80-\uA4CF""" +
    """\uAC00-\uD7A3""" +
    """\uF900-\uFAFF""" +
    """\uFE10-\uFE19""" +
    """\uFE30-\uFE6F""" +
    """\uFF00-\uFF60""" +
    """\uFFE0-\uFFE6""" +
    // scalastyle:on nonascii
    """]""").r

  /**
   * Return the number of half widths in a given string. Note that a full width character
   * occupies two half widths.
   *
   * For a string consisting of 1 million characters, the execution of this method requires
   * about 50ms.
   */
  private def stringHalfWidth(str: String): Int = {
    if (str == null) 0 else str.length + fullWidthRegex.findAllIn(str).size
  }

  def format(title: String, header: Seq[String], rows: Seq[Seq[String]]): String = {
    val data = Seq(header).union(rows)
    val sb = new StringBuilder
    val numCols = header.size
    // We set a minimum column width at '10'
    val minimumColWidth = 10

    val colWidths = Array.fill(numCols)(minimumColWidth)
    for (row <- data) {
      for ((cell, i) <- row.zipWithIndex) {
        colWidths(i) = math.max(colWidths(i), stringHalfWidth(cell))
      }
    }

    val paddedRows = data.map { row =>
      row.zipWithIndex.map { case (cell, i) =>
        StringUtils.center(cell, colWidths(i))
      }
    }

    // Create SeparateLine
    val sep: String = colWidths.map("-" * _).addString(sb, "+", "+", "+\n").toString()

    val titleNewLine = "\n " + StringUtils.center(title, colWidths.sum) + "\n"

    // column names
    paddedRows.head.addString(sb, "|", "|", "|\n")
    sb.append(sep)

    // data
    paddedRows.tail.foreach(_.addString(sb, "|", "|", "|\n"))
    sb.append(sep)

    sb.append(s"${rows.size} row(s)\n")
    titleNewLine + sb.toString()
  }
}
