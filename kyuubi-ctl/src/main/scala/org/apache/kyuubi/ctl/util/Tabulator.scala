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

package org.apache.kyuubi.ctl.util

import com.jakewharton.fliptables.FlipTable
import org.apache.commons.lang3.StringUtils

private[kyuubi] object Tabulator {
  def format(title: String, header: Array[String], rows: Array[Array[String]]): String = {
    val textTable = formatTextTable(header, rows)
    val footer = s"${rows.size} row(s)\n"
    if (StringUtils.isBlank(title)) {
      textTable + footer
    } else {
      val rowWidth = textTable.split("\n").head.size
      val titleNewLine = "\n" + StringUtils.center(title, rowWidth) + "\n"
      titleNewLine + textTable + footer
    }
  }

  def formatTextTable(header: Array[String], rows: Array[Array[String]]): String = {
    val normalizedRows = rows.map(row => row.map(Option(_).getOrElse("N/A")))
    FlipTable.of(header, normalizedRows)
  }
}
