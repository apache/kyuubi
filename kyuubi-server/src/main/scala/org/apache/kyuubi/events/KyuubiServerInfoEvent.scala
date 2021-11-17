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

package org.apache.kyuubi.events

import org.apache.kyuubi

import org.apache.kyuubi.Utils
import org.apache.kyuubi.server.KyuubiServer
import org.apache.kyuubi.service.ServiceState

/**
 * A [[KyuubiServerInfoEvent]] is used to get server level info when KyuubiServer start or stop
 *
 * @param serverName the server name
 * @param startTime the time when the server started
 * @param eventTime the time when the event made
 * @param states the server states
 * @param serverIP the server ip
 * @param serverConf the server config
 * @param serverEnv the server environment
 * @param extraStatesInfo the server extra explain of states
 */
case class KyuubiServerInfoEvent private(
    serverName: String,
    startTime: Long,
    eventTime: Long,
    states: String,
    serverIP: String,
    serverConf: Map[String, String],
    serverEnv: Map[String, String],
    extraStatesInfo: String) extends KyuubiServerEvent {

  val BUILD_USER: String = kyuubi.BUILD_USER
  val BUILD_DATE: String = kyuubi.BUILD_DATE
  val REPO_URL: String = kyuubi.REPO_URL

  val VERSION_INFO = Map(
    "KYUUBI_VERSION"         -> kyuubi.KYUUBI_VERSION,
    "JAVA_COMPILE_VERSION"   -> kyuubi.JAVA_COMPILE_VERSION,
    "SCALA_COMPILE_VERSION"  -> kyuubi.SCALA_COMPILE_VERSION,
    "SPARK_COMPILE_VERSION"  -> kyuubi.SPARK_COMPILE_VERSION,
    "HIVE_COMPILE_VERSION"   -> kyuubi.HIVE_COMPILE_VERSION,
    "HADOOP_COMPILE_VERSION" -> kyuubi.HADOOP_COMPILE_VERSION)

  override def partitions: Seq[(String, String)] =
    ("day", Utils.getDateFromTimestamp(startTime)) :: Nil
}

object KyuubiServerInfoEvent {
  def apply(server: KyuubiServer, extraStatesInfo: String): KyuubiServerInfoEvent = {
    val serverState = server.getServiceState
    serverState match {
      // Only server is started that we can log event
      case ServiceState.STARTED =>
        KyuubiServerInfoEvent(
          server.getName,
          server.getStartTime,
          System.currentTimeMillis(),
          serverState.toString,
          server.frontendServices.head.connectionUrl,
          server.getConf.getAll,
          server.getConf.getEnvs,
          extraStatesInfo
        )
      case _ => null
    }
  }
}
