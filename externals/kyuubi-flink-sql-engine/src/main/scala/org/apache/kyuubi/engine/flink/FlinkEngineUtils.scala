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

package org.apache.kyuubi.engine.flink

import org.apache.flink.runtime.util.EnvironmentInformation
import org.apache.flink.table.client.SqlClientException
import org.apache.flink.table.client.cli.{CliOptions, CliOptionsParser}
import org.apache.flink.table.client.gateway.context.SessionContext
import org.apache.flink.table.client.gateway.local.LocalExecutor

import org.apache.kyuubi.Logging

object FlinkEngineUtils extends Logging {

  val MODE_EMBEDDED = "embedded"

  def checkFlinkVersion(): Unit = {
    val flinkVersion = EnvironmentInformation.getVersion
    if (!flinkVersion.startsWith("1.14")) {
      throw new RuntimeException("Only Flink-1.14.x is supported now!")
    }
  }

  def parseCliOptions(args: Array[String]): CliOptions = {
    val (mode, modeArgs) =
      if (args.isEmpty || args(0).startsWith("-")) (MODE_EMBEDDED, args)
      else (args(0), args.drop(1))
    // TODO remove requirement of flink-python
    val options = CliOptionsParser.parseEmbeddedModeClient(modeArgs)
    mode match {
      case MODE_EMBEDDED if options.isPrintHelp => CliOptionsParser.printHelpEmbeddedModeClient()
      case MODE_EMBEDDED =>
      case _ => throw new SqlClientException("Other mode is not supported yet.")
    }
    options
  }

  def getSessionContext(localExecutor: LocalExecutor, sessionId: String): SessionContext = {
    val method = classOf[LocalExecutor].getMethod("getSessionContext", classOf[String])
    method.setAccessible(true)
    method.invoke(localExecutor, sessionId).asInstanceOf[SessionContext]
  }
}
