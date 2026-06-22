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

package org.apache.kyuubi.config

import org.apache.kyuubi.engine.EngineType

object ConfigAudience extends Enumeration {
  type ConfigAudience = Value

  val SERVER, SPARK, FLINK, HIVE, TRINO, JDBC, DATA_AGENT, ALL_ENGINES, ANY = Value

  private val ENGINE_VALUES: Set[Value] = Set(SPARK, FLINK, HIVE, TRINO, JDBC, DATA_AGENT)

  def fromEngineType(engineType: EngineType.EngineType): ConfigAudience = engineType match {
    case EngineType.SPARK_SQL => SPARK
    case EngineType.FLINK_SQL => FLINK
    case EngineType.HIVE_SQL => HIVE
    case EngineType.TRINO => TRINO
    case EngineType.JDBC => JDBC
    case EngineType.DATA_AGENT => DATA_AGENT
    case _ => throw new IllegalArgumentException(s"Unsupported engine type: $engineType")
  }

  def isAudienceMatch(
      configAudience: Set[ConfigAudience],
      target: ConfigAudience): Boolean = {
    configAudience.contains(ANY) ||
    (configAudience.contains(ALL_ENGINES) && ENGINE_VALUES.contains(target)) ||
    configAudience.contains(target)
  }

  def inferAudience(key: String): Set[ConfigAudience] = {
    if (key.startsWith("kyuubi.server.") || key.startsWith("kyuubi.frontend.") ||
      key.startsWith("kyuubi.backend.server.")) {
      Set(SERVER)
    } else if (key.startsWith("kyuubi.engine.spark.") ||
      key.startsWith("kyuubi.session.engine.spark.") || key.startsWith("spark.")) {
      Set(SPARK)
    } else if (key.startsWith("kyuubi.engine.flink.") ||
      key.startsWith("kyuubi.session.engine.flink.") || key.startsWith("flink.")) {
      Set(FLINK)
    } else if (key.startsWith("kyuubi.engine.hive.") ||
      key.startsWith("kyuubi.session.engine.hive.") || key.startsWith("hive.")) {
      Set(HIVE)
    } else if (key.startsWith("kyuubi.engine.trino.") ||
      key.startsWith("kyuubi.session.engine.trino.") || key.startsWith("trino.")) {
      Set(TRINO)
    } else if (key.startsWith("kyuubi.engine.jdbc.") ||
      key.startsWith("kyuubi.session.engine.jdbc.")) {
      Set(JDBC)
    } else if (key.startsWith("kyuubi.engine.data.agent.") ||
      key.startsWith("kyuubi.session.engine.data-agent.")) {
      Set(DATA_AGENT)
    } else {
      Set(ANY)
    }
  }
}
