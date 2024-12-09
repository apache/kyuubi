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

package org.apache.kyuubi.engine.chat

import java.io.File
import java.nio.file.{Files, Paths}

import scala.collection.mutable

import com.google.common.annotations.VisibleForTesting
import org.apache.commons.lang3.StringUtils

import org.apache.kyuubi.{Logging, SCALA_COMPILE_VERSION, Utils}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf._
import org.apache.kyuubi.config.KyuubiReservedKeys.KYUUBI_SESSION_USER_KEY
import org.apache.kyuubi.engine.ProcBuilder
import org.apache.kyuubi.operation.log.OperationLog
import org.apache.kyuubi.util.command.CommandLineUtils._

class ChatProcessBuilder(
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

  /**
   * The short name of the engine process builder, we use this for form the engine jar paths now
   * see `mainResource`
   */
  override def shortName: String = "chat"

  override protected def module: String = "kyuubi-chat-engine"

  /**
   * The class containing the main method
   */
  override protected def mainClass: String = "org.apache.kyuubi.engine.chat.ChatEngine"

  override protected val commands: Iterable[String] = {
    val buffer = new mutable.ListBuffer[String]()
    buffer += executable

    val memory = conf.get(ENGINE_CHAT_MEMORY)
    buffer += s"-Xmx$memory"

    val javaOptions = conf.get(ENGINE_CHAT_JAVA_OPTIONS).filter(StringUtils.isNotBlank(_))
    javaOptions.foreach(buffer += _)

    val classpathEntries = new mutable.LinkedHashSet[String]
    mainResource.foreach(classpathEntries.add)
    mainResource.foreach { path =>
      val parent = Paths.get(path).getParent
      val chatDevDepDir = parent
        .resolve(s"scala-$SCALA_COMPILE_VERSION")
        .resolve("jars")
      if (Files.exists(chatDevDepDir)) {
        // add dev classpath
        classpathEntries.add(s"$chatDevDepDir${File.separator}*")
      } else {
        // add prod classpath
        classpathEntries.add(s"$parent${File.separator}*")
      }
    }

    val extraCp = conf.get(ENGINE_CHAT_EXTRA_CLASSPATH)
    extraCp.foreach(classpathEntries.add)
    buffer ++= genClasspathOption(classpathEntries)

    buffer += mainClass

    buffer ++= confKeyValue(KYUUBI_SESSION_USER_KEY, proxyUser)

    buffer ++= confKeyValues(conf.getAll)

    buffer
  }

  override def toString: String = {
    if (commands == null) {
      super.toString
    } else {
      redactConfValues(
        Utils.redactCommandLineArgs(conf, commands),
        Set(ENGINE_CHAT_GPT_API_KEY.key)).map {
        case arg if arg.startsWith("-") || arg == mainClass => s"\\\n\t$arg"
        case arg => arg
      }.mkString(" ")
    }
  }
}
