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

import org.apache.kyuubi.KyuubiFunSuite
import org.apache.kyuubi.engine.EngineType

class ConfigAudienceSuite extends KyuubiFunSuite {

  test("fromEngineType") {
    assert(ConfigAudience.fromEngineType(EngineType.SPARK_SQL) === ConfigAudience.SPARK)
    assert(ConfigAudience.fromEngineType(EngineType.FLINK_SQL) === ConfigAudience.FLINK)
    assert(ConfigAudience.fromEngineType(EngineType.HIVE_SQL) === ConfigAudience.HIVE)
    assert(ConfigAudience.fromEngineType(EngineType.TRINO) === ConfigAudience.TRINO)
    assert(ConfigAudience.fromEngineType(EngineType.JDBC) === ConfigAudience.JDBC)
    assert(ConfigAudience.fromEngineType(EngineType.DATA_AGENT) === ConfigAudience.DATA_AGENT)
  }

  test("isAudienceMatch") {
    assert(ConfigAudience.isAudienceMatch(Set(ConfigAudience.ANY), ConfigAudience.SPARK))
    assert(ConfigAudience.isAudienceMatch(Set(ConfigAudience.ALL_ENGINES), ConfigAudience.FLINK))
    assert(!ConfigAudience.isAudienceMatch(Set(ConfigAudience.ALL_ENGINES), ConfigAudience.SERVER))
    assert(ConfigAudience.isAudienceMatch(Set(ConfigAudience.SPARK), ConfigAudience.SPARK))
    assert(!ConfigAudience.isAudienceMatch(Set(ConfigAudience.SPARK), ConfigAudience.FLINK))
    assert(!ConfigAudience.isAudienceMatch(Set(ConfigAudience.SERVER), ConfigAudience.SPARK))
  }

  test("inferAudience for server prefixes") {
    assert(ConfigAudience.inferAudience("kyuubi.server.name") === Set(ConfigAudience.SERVER))
    assert(ConfigAudience.inferAudience("kyuubi.frontend.bind.host") === Set(ConfigAudience.SERVER))
    assert(
      ConfigAudience.inferAudience("kyuubi.backend.server.event") === Set(ConfigAudience.SERVER))
  }

  test("inferAudience for kyuubi engine prefixes") {
    assert(
      ConfigAudience.inferAudience("kyuubi.engine.spark.main") === Set(ConfigAudience.SPARK))
    assert(
      ConfigAudience.inferAudience("kyuubi.engine.flink.main") === Set(ConfigAudience.FLINK))
    assert(
      ConfigAudience.inferAudience("kyuubi.engine.hive.main") === Set(ConfigAudience.HIVE))
    assert(
      ConfigAudience.inferAudience("kyuubi.engine.trino.main") === Set(ConfigAudience.TRINO))
    assert(
      ConfigAudience.inferAudience("kyuubi.engine.jdbc.main") === Set(ConfigAudience.JDBC))
    assert(
      ConfigAudience.inferAudience("kyuubi.engine.data.agent.main") ===
        Set(ConfigAudience.DATA_AGENT))
  }

  test("inferAudience for session engine prefixes") {
    assert(ConfigAudience.inferAudience("kyuubi.session.engine.spark.main.resource") ===
      Set(ConfigAudience.SPARK))
    assert(ConfigAudience.inferAudience("kyuubi.session.engine.flink.main.resource") ===
      Set(ConfigAudience.FLINK))
    assert(ConfigAudience.inferAudience("kyuubi.session.engine.hive.main.resource") ===
      Set(ConfigAudience.HIVE))
    assert(ConfigAudience.inferAudience("kyuubi.session.engine.trino.connection.url") ===
      Set(ConfigAudience.TRINO))
    assert(ConfigAudience.inferAudience("kyuubi.session.engine.jdbc.main.resource") ===
      Set(ConfigAudience.JDBC))
    assert(ConfigAudience.inferAudience("kyuubi.session.engine.data-agent.main.resource") ===
      Set(ConfigAudience.DATA_AGENT))
  }

  test("inferAudience for native engine prefixes") {
    assert(ConfigAudience.inferAudience("spark.executor.memory") === Set(ConfigAudience.SPARK))
    assert(ConfigAudience.inferAudience("spark.driver.cores") === Set(ConfigAudience.SPARK))
    assert(ConfigAudience.inferAudience("flink.execution.target") === Set(ConfigAudience.FLINK))
    assert(ConfigAudience.inferAudience("hive.exec.parallel") === Set(ConfigAudience.HIVE))
    assert(ConfigAudience.inferAudience("trino.catalog") === Set(ConfigAudience.TRINO))
  }

  test("inferAudience defaults to ANY") {
    assert(
      ConfigAudience.inferAudience("kyuubi.operation.idle.timeout") === Set(ConfigAudience.ANY))
    assert(ConfigAudience.inferAudience("hadoop.security.authentication") ===
      Set(ConfigAudience.ANY))
    assert(ConfigAudience.inferAudience("some.random.key") === Set(ConfigAudience.ANY))
  }

}
