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

package org.apache.kyuubi.ctl.commands.common

import java.lang.{StringBuilder => Builder}
import java.util.List

import scala.collection.JavaConverters._

import com.beust.jcommander.{DefaultUsageFormatter, JCommander, ParameterDescription}

class UnixStyleUsage(commander: JCommander) extends DefaultUsageFormatter(commander) {

  override def appendMainLine(out: Builder, hasOptions: Boolean,
      hasCommands: Boolean, indentCount: Int, indent: String): Unit = {
    wrapDescription(out, indentCount, commander.getProgramDisplayName + "\n")
  }

  override def appendAllParametersDetails(out: Builder, indentCount: Int, indent: String,
      sortedParameters: List[ParameterDescription]): Unit = {
    var prefixIndent = 0
    for (pd <- sortedParameters.asScala) {
      val prefix = "  " + pd.getNames
      if (prefix.length > prefixIndent) prefixIndent = prefix.length
    }

    for (pd <- sortedParameters.asScala) {
      val prefix = "  " + pd.getNames
      out.append(indent)
        .append(prefix)
        .append(DefaultUsageFormatter.s(prefixIndent-prefix.length))
        .append(" ")

      val initialLinePrefixLength = indent.length + prefixIndent + 3;
      val description = pd.getDescription
      wrapDescription(out, indentCount + prefixIndent - 3, initialLinePrefixLength, description)
      out.append("\n")
    }
    out.deleteCharAt(out.length - 1)
  }
}
