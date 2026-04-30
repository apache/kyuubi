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

package org.apache.kyuubi.engine.dataagent

import java.io.File
import java.nio.file.{Files, Paths}

import scala.collection.mutable

import com.google.common.annotations.VisibleForTesting
import org.apache.commons.lang3.StringUtils

import org.apache.kyuubi.{Logging, SCALA_COMPILE_VERSION, Utils}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf._
import org.apache.kyuubi.config.KyuubiReservedKeys.{KYUUBI_ENGINE_ID, KYUUBI_SESSION_USER_KEY}
import org.apache.kyuubi.engine.ProcBuilder
import org.apache.kyuubi.operation.log.OperationLog
import org.apache.kyuubi.util.command.CommandLineUtils._

class DataAgentProcessBuilder(
    override val proxyUser: String,
    override val doAsEnabled: Boolean,
    override val conf: KyuubiConf,
    val engineRefId: String,
    val extraEngineLog: Option[OperationLog] = None)
  extends ProcBuilder with Logging {

  @VisibleForTesting
  def this(proxyUser: String, doAsEnabled: Boolean, conf: KyuubiConf) = {
    this(proxyUser, doAsEnabled, conf, "")
  }

  override def shortName: String = "data-agent"

  override protected def module: String = "kyuubi-data-agent-engine"

  override protected def mainClass: String =
    "org.apache.kyuubi.engine.dataagent.DataAgentEngine"

  override protected val commands: Iterable[String] = {
    val buffer = new mutable.ListBuffer[String]()
    buffer += executable

    val memory = conf.get(ENGINE_DATA_AGENT_MEMORY)
    buffer += s"-Xmx$memory"
    buffer += "-Dfile.encoding=UTF-8"

    val javaOptions = conf.get(ENGINE_DATA_AGENT_JAVA_OPTIONS).filter(StringUtils.isNotBlank(_))
    if (javaOptions.isDefined) {
      buffer ++= parseOptionString(javaOptions.get)
    }

    val classpathEntries = new mutable.LinkedHashSet[String]
    mainResource.foreach(classpathEntries.add)
    mainResource.foreach { path =>
      val parent = Paths.get(path).getParent
      val devDepDir = parent
        .resolve(s"scala-$SCALA_COMPILE_VERSION")
        .resolve("jars")
      if (Files.exists(devDepDir)) {
        // add dev classpath
        classpathEntries.add(s"$devDepDir${File.separator}*")
      } else {
        // add prod classpath
        classpathEntries.add(s"$parent${File.separator}*")
      }
    }

    val extraCp = conf.get(ENGINE_DATA_AGENT_EXTRA_CLASSPATH)
    extraCp.foreach(classpathEntries.add)
    buffer ++= genClasspathOption(classpathEntries)

    buffer += mainClass

    buffer ++= confKeyValue(KYUUBI_SESSION_USER_KEY, proxyUser)
    buffer ++= confKeyValue(KYUUBI_ENGINE_ID, engineRefId)

    buffer ++= confKeyValues(conf.getAll)

    buffer
  }

  override def toString: String = {
    if (commands == null) {
      super.toString
    } else {
      redactConfValues(
        Utils.redactCommandLineArgs(conf, commands),
        Set(ENGINE_DATA_AGENT_OPENAI_API_KEY.key)).map {
        case arg if arg.startsWith("-") || arg == mainClass => s"\\\n\t$arg"
        case arg => arg
      }.mkString(" ")
    }
  }
}
