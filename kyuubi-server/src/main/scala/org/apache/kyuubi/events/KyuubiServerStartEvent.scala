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

import org.apache.kyuubi.{KYUUBI_VERSION, Utils}
import org.apache.kyuubi.server.KyuubiServer

/**
 * A [[KyuubiServerStartEvent]] is used to get server level info when KyuubiServer start
 *
 * @param serverName the server name
 * @param startTime the time the server start
 * @param states the server states
 * @param serverConf the server config
 * @param serverEnv the server environment
 */
case class KyuubiServerStartEvent private(
    serverName: String,
    startTime: Long,
    states: String,
    serverConf: Map[String, String],
    serverEnv: Map[String, String]) extends KyuubiServerEvent {

  val serverVersion: String = KYUUBI_VERSION

  override def partitions: Seq[(String, String)] =
    ("day", Utils.getDateFromTimestamp(startTime)) :: Nil
}

object KyuubiServerStartEvent {
  def apply(server: KyuubiServer): KyuubiServerStartEvent = {
    new KyuubiServerStartEvent(
      server.getName,
      server.getStartTime,
      server.getServiceState.toString,
      server.getConf.getAll,
      server.getConf.getEnvs
    )
  }
}
