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

import scala.collection.convert.ImplicitConversions._

import org.apache.commons.cli.{CommandLine, DefaultParser, Option, Options, ParseException}
import org.apache.flink.api.common.JobID
import org.apache.flink.core.fs.Path
import org.apache.flink.runtime.util.EnvironmentInformation
import org.apache.flink.table.client.SqlClientException
import org.apache.flink.table.client.cli.{CliClient, CliOptions, CliOptionsParser}
import org.apache.flink.table.client.cli.CliOptionsParser._
import org.apache.flink.table.gateway.service.context.SessionContext
import org.apache.flink.table.gateway.service.result.ResultFetcher
import org.apache.flink.table.gateway.service.session.Session

import org.apache.kyuubi.Logging
import org.apache.kyuubi.engine.SemanticVersion

object FlinkEngineUtils extends Logging {

  val MODE_EMBEDDED = "embedded"
  val EMBEDDED_MODE_CLIENT_OPTIONS: Options = getEmbeddedModeClientOptions(new Options);

  val SUPPORTED_FLINK_VERSIONS: Array[SemanticVersion] =
    Array("1.16", "1.17").map(SemanticVersion.apply)

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

  def parseCliOptions(args: Array[String]): CliOptions.EmbeddedCliOptions = {
    val (mode, modeArgs) =
      if (args.isEmpty || args(0).startsWith("-")) (MODE_EMBEDDED, args)
      else (args(0), args.drop(1))
    val options = parseEmbeddedModeClient(modeArgs)
    if (mode == MODE_EMBEDDED) {
      if (options.isPrintHelp) {
        printHelpEmbeddedModeClient(CliClient.DEFAULT_TERMINAL_FACTORY.get().writer())
      }
      options
    } else {
      throw new NotImplementedError("Standalone Flink SQL gateway mode is not supported yet.")
    }
  }

  /**
   * Copied and modified from [[org.apache.flink.table.client.cli.CliOptionsParser]]
   * to avoid loading flink-python classes which we doesn't support yet.
   */
  def parseEmbeddedModeClient(args: Array[String]): CliOptions.EmbeddedCliOptions =
    try {
      val parser = new DefaultParser
      val line = parser.parse(EMBEDDED_MODE_CLIENT_OPTIONS, args, true)
      new CliOptions.EmbeddedCliOptions(
        line.hasOption(CliOptionsParser.OPTION_HELP.getOpt),
        checkSessionId(line),
        checkUrl(line, CliOptionsParser.OPTION_INIT_FILE),
        checkUrl(line, CliOptionsParser.OPTION_FILE),
        line.getOptionValue(CliOptionsParser.OPTION_UPDATE.getOpt),
        line.getOptionValue(CliOptionsParser.OPTION_HISTORY.getOpt),
        checkUrls(line, CliOptionsParser.OPTION_JAR),
        checkUrls(line, CliOptionsParser.OPTION_LIBRARY),
        null)
    } catch {
      case e: ParseException =>
        throw new SqlClientException(e.getMessage)
    }

  def getSessionContext(session: Session): SessionContext = {
    val field = classOf[Session].getDeclaredField("sessionContext")
    field.setAccessible(true);
    field.get(session).asInstanceOf[SessionContext]
  }

  def getResultJobId(resultFetch: ResultFetcher): JobID = {
    val field = classOf[ResultFetcher].getDeclaredField("jobID")
    field.setAccessible(true);
    try {
      field.get(resultFetch).asInstanceOf[JobID]
    } catch {
      case _: NullPointerException => null
      case e: Throwable =>
        throw new IllegalStateException("Unexpected error occurred while fetching query ID", e)
    }
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
