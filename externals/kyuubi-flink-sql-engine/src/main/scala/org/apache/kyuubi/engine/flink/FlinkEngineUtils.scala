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

import java.io.File
import java.net.URL

import scala.collection.JavaConverters._

import org.apache.commons.cli.{CommandLine, DefaultParser, Option, Options, ParseException}
import org.apache.flink.core.fs.Path
import org.apache.flink.runtime.util.EnvironmentInformation
import org.apache.flink.table.client.SqlClientException
import org.apache.flink.table.client.cli.CliOptions
import org.apache.flink.table.client.cli.CliOptionsParser._
import org.apache.flink.table.client.gateway.context.SessionContext
import org.apache.flink.table.client.gateway.local.LocalExecutor

import org.apache.kyuubi.Logging
import org.apache.kyuubi.engine.SemanticVersion

object FlinkEngineUtils extends Logging {

  val MODE_EMBEDDED = "embedded"
  val EMBEDDED_MODE_CLIENT_OPTIONS: Options = getEmbeddedModeClientOptions(new Options);

  val SUPPORTED_FLINK_VERSIONS: Array[SemanticVersion] =
    Array("1.14", "1.15", "1.16").map(SemanticVersion.apply)

  def checkFlinkVersion(): Unit = {
    val flinkVersion = EnvironmentInformation.getVersion
    if (SUPPORTED_FLINK_VERSIONS.contains(SemanticVersion(flinkVersion))) {
      info(s"The current Flink version is $flinkVersion")
    } else {
      throw new UnsupportedOperationException(
        s"You are using unsupported Flink version $flinkVersion, " +
          s"only Flink ${SUPPORTED_FLINK_VERSIONS.mkString(", ")} are supported now.")
    }
  }

  def isFlinkVersionAtMost(targetVersionString: String): Boolean =
    SemanticVersion(EnvironmentInformation.getVersion).isVersionAtMost(targetVersionString)

  def isFlinkVersionAtLeast(targetVersionString: String): Boolean =
    SemanticVersion(EnvironmentInformation.getVersion).isVersionAtLeast(targetVersionString)

  def isFlinkVersionEqualTo(targetVersionString: String): Boolean =
    SemanticVersion(EnvironmentInformation.getVersion).isVersionEqualTo(targetVersionString)

  def parseCliOptions(args: Array[String]): CliOptions = {
    val (mode, modeArgs) =
      if (args.isEmpty || args(0).startsWith("-")) (MODE_EMBEDDED, args)
      else (args(0), args.drop(1))
    val options = parseEmbeddedModeClient(modeArgs)
    if (mode == MODE_EMBEDDED) {
      if (options.isPrintHelp) {
        printHelpEmbeddedModeClient()
      }
      options
    } else {
      throw new SqlClientException("Other mode is not supported yet.")
    }
  }

  def getSessionContext(localExecutor: LocalExecutor, sessionId: String): SessionContext = {
    val method = classOf[LocalExecutor].getDeclaredMethod("getSessionContext", classOf[String])
    method.setAccessible(true)
    method.invoke(localExecutor, sessionId).asInstanceOf[SessionContext]
  }

  def parseEmbeddedModeClient(args: Array[String]): CliOptions =
    try {
      val parser = new DefaultParser
      val line = parser.parse(EMBEDDED_MODE_CLIENT_OPTIONS, args, true)
      val jarUrls = checkUrls(line, OPTION_JAR)
      val libraryUrls = checkUrls(line, OPTION_LIBRARY)
      new CliOptions(
        line.hasOption(OPTION_HELP.getOpt),
        checkSessionId(line),
        checkUrl(line, OPTION_INIT_FILE),
        checkUrl(line, OPTION_FILE),
        if (jarUrls != null && jarUrls.nonEmpty) jarUrls.asJava else null,
        if (libraryUrls != null && libraryUrls.nonEmpty) libraryUrls.asJava else null,
        line.getOptionValue(OPTION_UPDATE.getOpt),
        line.getOptionValue(OPTION_HISTORY.getOpt),
        null)
    } catch {
      case e: ParseException =>
        throw new SqlClientException(e.getMessage)
    }

  def checkSessionId(line: CommandLine): String = {
    val sessionId = line.getOptionValue(OPTION_SESSION.getOpt)
    if (sessionId != null && !sessionId.matches("[a-zA-Z0-9_\\-.]+")) {
      throw new SqlClientException("Session identifier must only consists of 'a-zA-Z0-9_-.'.")
    } else sessionId
  }

  def checkUrl(line: CommandLine, option: Option): URL = {
    val urls: List[URL] = checkUrls(line, option)
    if (urls != null && urls.nonEmpty) urls.head
    else null
  }

  def checkUrls(line: CommandLine, option: Option): List[URL] = {
    if (line.hasOption(option.getOpt)) {
      line.getOptionValues(option.getOpt).distinct.map((url: String) => {
        checkFilePath(url)
        try Path.fromLocalFile(new File(url).getAbsoluteFile).toUri.toURL
        catch {
          case e: Exception =>
            throw new SqlClientException(
              "Invalid path for option '" + option.getLongOpt + "': " + url,
              e)
        }
      }).toList
    } else null
  }
}
