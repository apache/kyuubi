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
import java.lang.{Boolean => JBoolean}
import java.net.URL
import java.util.{ArrayList => JArrayList, Collections => JCollections, List => JList}

import scala.collection.convert.ImplicitConversions._

import org.apache.commons.cli.{CommandLine, DefaultParser, Options}
import org.apache.flink.api.common.JobID
import org.apache.flink.configuration.Configuration
import org.apache.flink.core.fs.Path
import org.apache.flink.runtime.util.EnvironmentInformation
import org.apache.flink.table.client.SqlClientException
import org.apache.flink.table.client.cli.CliOptionsParser
import org.apache.flink.table.client.cli.CliOptionsParser._
import org.apache.flink.table.gateway.service.context.{DefaultContext, SessionContext}
import org.apache.flink.table.gateway.service.result.ResultFetcher
import org.apache.flink.table.gateway.service.session.Session
import org.apache.flink.util.JarUtils
import org.apache.hadoop.security.{Credentials, UserGroupInformation}

import org.apache.kyuubi.{KyuubiException, Logging}
import org.apache.kyuubi.util.{KyuubiHadoopUtils, SemanticVersion}
import org.apache.kyuubi.util.reflect.ReflectUtils._

object FlinkEngineUtils extends Logging {

  val EMBEDDED_MODE_CLIENT_OPTIONS: Options = getEmbeddedModeClientOptions(new Options)

  private def SUPPORTED_FLINK_VERSIONS =
    Set("1.17", "1.18", "1.19", "1.20").map(SemanticVersion.apply)

  val FLINK_RUNTIME_VERSION: SemanticVersion = SemanticVersion(EnvironmentInformation.getVersion)

  def checkFlinkVersion(): Unit = {
    val flinkVersion = EnvironmentInformation.getVersion
    if (SUPPORTED_FLINK_VERSIONS.contains(FLINK_RUNTIME_VERSION)) {
      info(s"The current Flink version is $flinkVersion")
    } else {
      throw new UnsupportedOperationException(
        s"You are using unsupported Flink version $flinkVersion, " +
          s"only Flink ${SUPPORTED_FLINK_VERSIONS.mkString(", ")} are supported now.")
    }
  }

  /**
   * Copied and modified from [[org.apache.flink.table.client.cli.CliOptionsParser]]
   * to avoid loading flink-python classes which we doesn't support yet.
   */
  private def discoverDependencies(
      jars: JList[URL],
      libraries: JList[URL]): JList[URL] = {
    val dependencies: JList[URL] = new JArrayList[URL]
    try { // find jar files
      for (url <- jars) {
        JarUtils.checkJarFile(url)
        dependencies.add(url)
      }
      // find jar files in library directories
      libraries.foreach { libUrl =>
        val dir: File = new File(libUrl.toURI)
        if (!dir.isDirectory) throw new SqlClientException(s"Directory expected: $dir")
        if (!dir.canRead) throw new SqlClientException(s"Directory cannot be read: $dir")
        val files: Array[File] = dir.listFiles
        if (files == null) throw new SqlClientException(s"Directory cannot be read: $dir")
        files.filter { f => f.isFile && f.getAbsolutePath.toLowerCase.endsWith(".jar") }
          .foreach { f =>
            val url: URL = f.toURI.toURL
            JarUtils.checkJarFile(url)
            dependencies.add(url)
          }
      }
    } catch {
      case e: Exception =>
        throw new SqlClientException("Could not load all required JAR files.", e)
    }
    dependencies
  }

  def getDefaultContext(
      args: Array[String],
      flinkConf: Configuration,
      flinkConfDir: String): DefaultContext = {
    val parser = new DefaultParser
    val line = parser.parse(EMBEDDED_MODE_CLIENT_OPTIONS, args, true)
    val jars: JList[URL] = Option(checkUrls(line, CliOptionsParser.OPTION_JAR))
      .getOrElse(JCollections.emptyList())
    val libDirs: JList[URL] = Option(checkUrls(line, CliOptionsParser.OPTION_LIBRARY))
      .getOrElse(JCollections.emptyList())
    val dependencies: JList[URL] = discoverDependencies(jars, libDirs)
    if (FLINK_RUNTIME_VERSION >= "1.19") {
      invokeAs[DefaultContext](
        classOf[DefaultContext],
        "load",
        (classOf[Configuration], flinkConf),
        (classOf[JList[URL]], dependencies),
        (classOf[Boolean], JBoolean.TRUE))
    } else if (FLINK_RUNTIME_VERSION >= "1.17") {
      invokeAs[DefaultContext](
        classOf[DefaultContext],
        "load",
        (classOf[Configuration], flinkConf),
        (classOf[JList[URL]], dependencies),
        (classOf[Boolean], JBoolean.TRUE),
        (classOf[Boolean], JBoolean.FALSE))
    } else {
      throw new KyuubiException(
        s"Flink version ${EnvironmentInformation.getVersion} are not supported currently.")
    }
  }

  def getSessionContext(session: Session): SessionContext = getField(session, "sessionContext")

  def getResultJobId(resultFetch: ResultFetcher): Option[JobID] = {
    try {
      Option(getField[JobID](resultFetch, "jobID"))
    } catch {
      case _: NullPointerException => None
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

  def checkUrl(line: CommandLine, option: org.apache.commons.cli.Option): URL = {
    val urls: JList[URL] = checkUrls(line, option)
    if (urls != null && urls.nonEmpty) urls.head
    else null
  }

  def checkUrls(line: CommandLine, option: org.apache.commons.cli.Option): JList[URL] = {
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

  def renewDelegationToken(delegationToken: String): Unit = {
    val newCreds = KyuubiHadoopUtils.decodeCredentials(delegationToken)
    val newTokens = KyuubiHadoopUtils.getTokenMap(newCreds)

    val updateCreds = new Credentials()
    val oldCreds = UserGroupInformation.getCurrentUser.getCredentials
    newTokens.foreach { case (alias, newToken) =>
      val oldToken = oldCreds.getToken(alias)
      if (oldToken != null) {
        if (KyuubiHadoopUtils.compareIssueDate(newToken, oldToken) > 0) {
          updateCreds.addToken(alias, newToken)
        } else {
          warn(s"Ignore token with earlier issue date: $newToken")
        }
      } else {
        info(s"Add new unknown token $newToken")
        updateCreds.addToken(alias, newToken)
      }
    }

    if (updateCreds.numberOfTokens() > 0) {
      info("Update delegation tokens. " +
        s"The number of tokens sent by the server is ${newCreds.numberOfTokens()}. " +
        s"The actual number of updated tokens is ${updateCreds.numberOfTokens()}.")
      UserGroupInformation.getCurrentUser.addCredentials(updateCreds)
    }
  }
}
