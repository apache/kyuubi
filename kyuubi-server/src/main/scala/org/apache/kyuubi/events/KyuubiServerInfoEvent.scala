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

import org.apache.kyuubi.{BUILD_DATE => P_BUILD_DATE, BUILD_USER => P_BUILD_USER, REPO_URL => P_REPO_URL, _}
import org.apache.kyuubi.server.KyuubiServer
import org.apache.kyuubi.service.ServiceState

/**
 * A [[KyuubiServerInfoEvent]] is used to get server level info when KyuubiServer start or stop
 *
 * @param serverName the server name
 * @param startTime the time when the server started
 * @param eventTime the time when the event made
 * @param state the server states
 * @param serverIP the server ip
 * @param serverConf the server config
 * @param serverEnv the server environment
 */
case class KyuubiServerInfoEvent private (
    serverName: String,
    startTime: Long,
    eventTime: Long,
    state: String,
    serverIP: String,
    serverConf: Map[String, String],
    serverEnv: Map[String, String]) extends KyuubiEvent {

  val BUILD_USER: String = P_BUILD_USER
  val BUILD_DATE: String = P_BUILD_DATE
  val REPO_URL: String = P_REPO_URL

  val VERSION_INFO = Map(
    "KYUUBI_VERSION" -> KYUUBI_VERSION,
    "JAVA_COMPILE_VERSION" -> JAVA_COMPILE_VERSION,
    "SCALA_COMPILE_VERSION" -> SCALA_COMPILE_VERSION,
    "SPARK_COMPILE_VERSION" -> SPARK_COMPILE_VERSION,
    "HIVE_COMPILE_VERSION" -> HIVE_COMPILE_VERSION,
    "HADOOP_COMPILE_VERSION" -> HADOOP_COMPILE_VERSION)

  override def partitions: Seq[(String, String)] =
    ("day", Utils.getDateFromTimestamp(startTime)) :: Nil
}

object KyuubiServerInfoEvent {

  def apply(
      server: KyuubiServer,
      state: ServiceState.ServiceState): Option[KyuubiServerInfoEvent] = {
    server.getServiceState match {
      // Only server is started that we can log event
      case ServiceState.STARTED =>
        Some(KyuubiServerInfoEvent(
          server.getName,
          server.getStartTime,
          System.currentTimeMillis(),
          state.toString,
          server.frontendServices.head.connectionUrl,
          server.getConf.getAll,
          server.getConf.getEnvs))
      case _ => None
    }
  }
}
