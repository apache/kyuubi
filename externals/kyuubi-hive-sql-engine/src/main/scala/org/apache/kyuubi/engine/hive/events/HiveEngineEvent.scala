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
package org.apache.kyuubi.engine.hive.events

import org.apache.kyuubi.Utils
import org.apache.kyuubi.engine.hive.HiveSQLEngine
import org.apache.kyuubi.events.KyuubiEvent
import org.apache.kyuubi.service.ServiceState
import org.apache.kyuubi.service.ServiceState.ServiceState

case class HiveEngineEvent(
    connectionUrl: String,
    startTime: Long,
    endTime: Long,
    state: ServiceState,
    diagnostic: String,
    settings: Map[String, String]) extends KyuubiEvent {

  override def partitions: Seq[(String, String)] = {
    ("day", Utils.getDateFromTimestamp(startTime)) :: Nil
  }

  override def toString: String = {
    s"""
       |HiveEngineEvent: {
       |connectionUrl: $connectionUrl,
       |startTime: $startTime,
       |endTime: $endTime,
       |state: $state,
       |diagnostic: $diagnostic,
       |settings: ${settings.mkString("<", ",", ">")}
       |}
       |""".stripMargin
  }
}

object HiveEngineEvent {

  def apply(engine: HiveSQLEngine): HiveEngineEvent = {
    val connectionUrl =
      if (engine.getServiceState.equals(ServiceState.LATENT)) {
        null
      } else {
        engine.frontendServices.head.connectionUrl
      }

    new HiveEngineEvent(
      connectionUrl = connectionUrl,
      startTime = engine.engineStartTime,
      endTime = -1L,
      state = engine.getServiceState,
      diagnostic = "",
      settings = engine.getConf.getAll)
  }
}
