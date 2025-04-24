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

package org.apache.kyuubi.engine

import java.nio.file.Paths

import scala.sys.process._

import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.server.metadata.MetadataManager

class JpsApplicationOperation extends ApplicationOperation {
  import ApplicationOperation._

  private var runner: String = _

  override def initialize(conf: KyuubiConf, metadataManager: Option[MetadataManager]): Unit = {
    val jps = sys.env.get("JAVA_HOME").orElse(sys.props.get("java.home"))
      .map(Paths.get(_, "bin", "jps").toString)
      .getOrElse("jps")
    runner =
      try {
        jps.!!
        jps
      } catch {
        case _: Throwable => null
      }
  }

  override def isSupported(appMgrInfo: ApplicationManagerInfo): Boolean = {
    runner != null &&
    (appMgrInfo.resourceManager.isEmpty || appMgrInfo.resourceManager.get == "local")
  }

  private def getEngine(tag: String): Option[String] = {
    if (runner == null) {
      None
    } else {
      val pb = s"$runner -ml" #| s"grep $tag"
      try {
        pb.lineStream_!.headOption
      } catch {
        case _: Throwable => None
      }
    }
  }

  private def killJpsApplicationByTag(tag: String, retryable: Boolean): KillResponse = {
    val commandOption = getEngine(tag)
    if (commandOption.nonEmpty) {
      val idAndCmd = commandOption.get
      val (id, _) = idAndCmd.splitAt(idAndCmd.indexOf(" "))
      try {
        s"kill -15 $id".lineStream
        (true, s"Succeeded to terminate: $idAndCmd")
      } catch {
        case e: Exception =>
          // the application might generate multiple processes, ensure that it is killed eventually.
          if (retryable && getEngine(tag).nonEmpty) {
            killJpsApplicationByTag(tag, false)
          } else {
            (false, s"Failed to terminate: $idAndCmd, due to ${e.getMessage}")
          }
      }
    } else {
      (false, NOT_FOUND)
    }
  }

  override def killApplicationByTag(
      appMgrInfo: ApplicationManagerInfo,
      tag: String,
      proxyUser: Option[String] = None): KillResponse = {
    killJpsApplicationByTag(tag, true)
  }

  override def getApplicationInfoByTag(
      appMgrInfo: ApplicationManagerInfo,
      tag: String,
      proxyUser: Option[String] = None,
      submitTime: Option[Long] = None): ApplicationInfo = {
    val commandOption = getEngine(tag)
    if (commandOption.nonEmpty) {
      val idAndCmd = commandOption.get
      val (id, cmd) = idAndCmd.splitAt(idAndCmd.indexOf(" "))
      ApplicationInfo(id = id, name = cmd, state = ApplicationState.RUNNING)
    } else {
      ApplicationInfo(id = null, name = null, state = ApplicationState.NOT_FOUND)
    }
    // TODO check if the process is zombie
  }

  override def stop(): Unit = {}
}
