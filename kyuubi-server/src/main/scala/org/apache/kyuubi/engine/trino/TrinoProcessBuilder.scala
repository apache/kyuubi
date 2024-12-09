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

package org.apache.kyuubi.engine.trino

import java.io.File
import java.nio.file.Paths

import scala.collection.mutable

import com.google.common.annotations.VisibleForTesting
import org.apache.commons.lang3.StringUtils

import org.apache.kyuubi.{Logging, SCALA_COMPILE_VERSION, Utils}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf._
import org.apache.kyuubi.config.KyuubiReservedKeys.KYUUBI_SESSION_USER_KEY
import org.apache.kyuubi.engine.{KyuubiApplicationManager, ProcBuilder}
import org.apache.kyuubi.operation.log.OperationLog
import org.apache.kyuubi.util.command.CommandLineUtils._

class TrinoProcessBuilder(
    override val proxyUser: String,
    override val doAsEnabled: Boolean,
    override val conf: KyuubiConf,
    val engineRefId: String,
    val extraEngineLog: Option[OperationLog] = None)
  extends ProcBuilder with Logging {

  @VisibleForTesting
  def this(proxyUser: String, doAsEnabled: Boolean, conf: KyuubiConf) {
    this(proxyUser, doAsEnabled, conf, "")
  }

  override protected def module: String = "kyuubi-trino-engine"

  override protected def mainClass: String = "org.apache.kyuubi.engine.trino.TrinoSqlEngine"

  override protected val commands: Iterable[String] = {
    KyuubiApplicationManager.tagApplication(engineRefId, shortName, clusterManager(), conf)
    require(
      conf.get(ENGINE_TRINO_CONNECTION_URL).nonEmpty,
      s"Trino server url can not be null! Please set ${ENGINE_TRINO_CONNECTION_URL.key}")
    require(
      conf.get(ENGINE_TRINO_CONNECTION_CATALOG).nonEmpty,
      s"Trino default catalog can not be null! Please set ${ENGINE_TRINO_CONNECTION_CATALOG.key}")
    val buffer = new mutable.ListBuffer[String]()
    buffer += executable

    val memory = conf.get(ENGINE_TRINO_MEMORY)
    buffer += s"-Xmx$memory"
    val javaOptions = conf.get(ENGINE_TRINO_JAVA_OPTIONS).filter(StringUtils.isNotBlank(_))
    if (javaOptions.isDefined) {
      buffer ++= parseOptionString(javaOptions.get)
    }

    val classpathEntries = new mutable.LinkedHashSet[String]
    // trino engine runtime jar
    mainResource.foreach(classpathEntries.add)

    mainResource.foreach { path =>
      val parent = Paths.get(path).getParent
      if (Utils.isTesting) {
        // add dev classpath
        val trinoDeps = parent
          .resolve(s"scala-$SCALA_COMPILE_VERSION")
          .resolve("jars")
        classpathEntries.add(s"$trinoDeps${File.separator}*")
      } else {
        // add prod classpath
        classpathEntries.add(s"$parent${File.separator}*")
      }
    }

    val extraCp = conf.get(ENGINE_TRINO_EXTRA_CLASSPATH)
    extraCp.foreach(classpathEntries.add)

    buffer ++= genClasspathOption(classpathEntries)

    buffer += mainClass

    // TODO: How shall we deal with proxyUser,
    // user.name
    // kyuubi.session.user
    // or just leave it, because we can handle it at operation layer
    buffer ++= confKeyValue(KYUUBI_SESSION_USER_KEY, proxyUser)

    buffer ++= confKeyValues(conf.getAll)

    buffer
  }

  override def shortName: String = "trino"

  override def toString: String = {
    if (commands == null) {
      super.toString
    } else {
      redactConfValues(
        Utils.redactCommandLineArgs(conf, commands),
        Set(
          ENGINE_TRINO_CONNECTION_PASSWORD.key,
          ENGINE_TRINO_CONNECTION_KEYSTORE_PASSWORD.key,
          ENGINE_TRINO_CONNECTION_TRUSTSTORE_PASSWORD.key)).map {
        case arg if arg.startsWith("-") => s"\\\n\t$arg"
        case arg => arg
      }.mkString(" ")
    }
  }
}
