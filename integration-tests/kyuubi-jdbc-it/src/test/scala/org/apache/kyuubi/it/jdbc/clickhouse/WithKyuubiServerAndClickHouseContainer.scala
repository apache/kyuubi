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
package org.apache.kyuubi.it.jdbc.clickhouse

import java.nio.file.{Files, Path, Paths}
import java.time.Duration

import org.apache.kyuubi.WithKyuubiServer
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf.{ENGINE_IDLE_TIMEOUT, ENGINE_JDBC_EXTRA_CLASSPATH, KYUUBI_ENGINE_ENV_PREFIX, KYUUBI_HOME_ENV_VAR_NAME}
import org.apache.kyuubi.engine.jdbc.clickhouse.WithClickHouseEngine
import org.apache.kyuubi.util.JavaUtils

trait WithKyuubiServerAndClickHouseContainer extends WithKyuubiServer with WithClickHouseEngine {

  private val kyuubiHome: String =
    JavaUtils.getCodeSourceLocation(getClass).split("integration-tests").head

  private val clickHouseJdbcConnectorPath: String = {
    val keyword = "clickhouse-jdbc"

    val jarsDir = Paths.get(kyuubiHome)
      .resolve("integration-tests")
      .resolve("kyuubi-jdbc-it")
      .resolve("target")

    Files.list(jarsDir)
      .filter { p: Path => p.getFileName.toString contains keyword }
      .findFirst
      .orElseThrow { () => new IllegalStateException(s"Can not find $keyword in $jarsDir.") }
      .toAbsolutePath
      .toString
  }

  override protected val conf: KyuubiConf = {
    KyuubiConf()
      .set(s"$KYUUBI_ENGINE_ENV_PREFIX.$KYUUBI_HOME_ENV_VAR_NAME", kyuubiHome)
      .set(ENGINE_JDBC_EXTRA_CLASSPATH, clickHouseJdbcConnectorPath)
      .set(ENGINE_IDLE_TIMEOUT, Duration.ofMinutes(1).toMillis)
  }

  override def beforeAll(): Unit = {
    val configs = withKyuubiConf
    configs.foreach(config => conf.set(config._1, config._2))
    super.beforeAll()
  }
}
