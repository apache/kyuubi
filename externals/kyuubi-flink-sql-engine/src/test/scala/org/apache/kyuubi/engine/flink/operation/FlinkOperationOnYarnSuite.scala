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

package org.apache.kyuubi.engine.flink.operation

import java.util.UUID

import org.apache.kyuubi.config.KyuubiConf.{ENGINE_SHARE_LEVEL, ENGINE_TYPE}
import org.apache.kyuubi.engine.ShareLevel
import org.apache.kyuubi.engine.flink.{WithDiscoveryFlinkSQLEngine, WithFlinkSQLEngineOnYarn}
import org.apache.kyuubi.ha.HighAvailabilityConf.{HA_ENGINE_REF_ID, HA_NAMESPACE}

class FlinkOperationOnYarnSuite extends FlinkOperationSuite
  with WithDiscoveryFlinkSQLEngine with WithFlinkSQLEngineOnYarn {

  protected def jdbcUrl: String = getFlinkEngineServiceUrl

  override def withKyuubiConf: Map[String, String] = {
    Map(
      HA_NAMESPACE.key -> namespace,
      HA_ENGINE_REF_ID.key -> engineRefId,
      ENGINE_TYPE.key -> "FLINK_SQL",
      ENGINE_SHARE_LEVEL.key -> shareLevel) ++ testExtraConf
  }

  override protected def engineRefId: String = UUID.randomUUID().toString

  def namespace: String = "/kyuubi/flink-yarn-application-test"

  def shareLevel: String = ShareLevel.USER.toString

  def engineType: String = "flink"
}
