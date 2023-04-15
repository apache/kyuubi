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
import java.util

import scala.collection.JavaConverters._
import scala.collection.convert.ImplicitConversions._

import org.apache.commons.cli.{CommandLine, DefaultParser, Options}
import org.apache.flink.api.common.JobID
import org.apache.flink.client.cli.{CustomCommandLine, DefaultCLI, GenericCLI}
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

import org.apache.kyuubi.{KyuubiException, Logging}
import org.apache.kyuubi.engine.SemanticVersion
import org.apache.kyuubi.reflection.{DynConstructors, DynFields, DynMethods}

object FlinkEngineUtils extends Logging {

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

  /**
   * Copied and modified from [[org.apache.flink.table.client.cli.CliOptionsParser]]
   * to avoid loading flink-python classes which we doesn't support yet.
   */
  private def discoverDependencies(
      jars: util.List[URL],
      libraries: util.List[URL]): util.List[URL] = {
    val dependencies: util.List[URL] = new util.ArrayList[URL]
    try { // find jar files
      for (url <- jars) {
        JarUtils.checkJarFile(url)
        dependencies.add(url)
      }
      // find jar files in library directories
      for (libUrl <- libraries) {
        val dir: File = new File(libUrl.toURI)
        if (!dir.isDirectory) throw new SqlClientException("Directory expected: " + dir)
        else if (!dir.canRead) throw new SqlClientException("Directory cannot be read: " + dir)
        val files: Array[File] = dir.listFiles
        if (files == null) throw new SqlClientException("Directory cannot be read: " + dir)
        for (f <- files) { // only consider jars
          if (f.isFile && f.getAbsolutePath.toLowerCase.endsWith(".jar")) {
            val url: URL = f.toURI.toURL
            JarUtils.checkJarFile(url)
            dependencies.add(url)
          }
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
    val jars: util.List[URL] = Option(checkUrls(line, CliOptionsParser.OPTION_JAR))
      .getOrElse(util.Collections.emptyList())
    val libDirs: util.List[URL] = Option(checkUrls(line, CliOptionsParser.OPTION_LIBRARY))
      .getOrElse(util.Collections.emptyList())
    val dependencies: util.List[URL] = discoverDependencies(jars, libDirs)
    if (FlinkEngineUtils.isFlinkVersionEqualTo("1.16")) {
      val commandLines: util.List[CustomCommandLine] =
        Seq(new GenericCLI(flinkConf, flinkConfDir), new DefaultCLI).asJava
      DynConstructors.builder()
        .impl(
          classOf[DefaultContext],
          classOf[Configuration],
          classOf[util.List[CustomCommandLine]])
        .build()
        .newInstance(flinkConf, commandLines)
        .asInstanceOf[DefaultContext]
    } else if (FlinkEngineUtils.isFlinkVersionEqualTo("1.17")) {
      DynMethods.builder("load")
        .impl(
          classOf[DefaultContext],
          classOf[Configuration],
          classOf[util.List[URL]],
          classOf[Boolean],
          classOf[Boolean])
        .buildStatic()
        .invoke[DefaultContext](
          flinkConf,
          dependencies,
          new java.lang.Boolean(true),
          new java.lang.Boolean(false))
    } else {
      throw new KyuubiException(
        s"Flink version ${EnvironmentInformation.getVersion} are not supported currently.")
    }
  }

  def getSessionContext(session: Session): SessionContext = {
    DynFields.builder()
      .hiddenImpl(classOf[Session], "sessionContext")
      .build()
      .get(session)
      .asInstanceOf[SessionContext]
  }

  def getResultJobId(resultFetch: ResultFetcher): JobID = {
    if (FlinkEngineUtils.isFlinkVersionAtMost("1.16")) {
      return null
    }
    try {
      DynFields.builder()
        .hiddenImpl(classOf[ResultFetcher], "jobID")
        .build()
        .get(resultFetch)
        .asInstanceOf[JobID]
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

  def checkUrl(line: CommandLine, option: org.apache.commons.cli.Option): URL = {
    val urls: util.List[URL] = checkUrls(line, option)
    if (urls != null && urls.nonEmpty) urls.head
    else null
  }

  def checkUrls(line: CommandLine, option: org.apache.commons.cli.Option): util.List[URL] = {
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
