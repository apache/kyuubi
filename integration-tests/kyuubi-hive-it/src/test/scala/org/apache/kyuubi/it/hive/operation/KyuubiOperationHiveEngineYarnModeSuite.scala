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
package org.apache.kyuubi.it.hive.operation

import org.apache.kyuubi.{HiveEngineTests, Utils, WithKyuubiServerAndHadoopMiniCluster}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf.{ENGINE_IDLE_TIMEOUT, ENGINE_TYPE, KYUUBI_ENGINE_ENV_PREFIX, KYUUBI_HOME_ENV_VAR_NAME}
import org.apache.kyuubi.engine.deploy.DeployMode

class KyuubiOperationHiveEngineYarnModeSuite extends HiveEngineTests
  with WithKyuubiServerAndHadoopMiniCluster {

  override protected val conf: KyuubiConf = {
    val metastore = Utils.createTempDir(prefix = getClass.getSimpleName)
    metastore.toFile.delete()
    KyuubiConf()
      .set(s"$KYUUBI_ENGINE_ENV_PREFIX.$KYUUBI_HOME_ENV_VAR_NAME", kyuubiHome)
      .set(ENGINE_TYPE, "HIVE_SQL")
      .set(KyuubiConf.ENGINE_HIVE_DEPLOY_MODE, DeployMode.YARN.toString)
      // increase this to 30s as hive session state and metastore client is slow initializing
      .setIfMissing(ENGINE_IDLE_TIMEOUT, 30000L)
      .set("javax.jdo.option.ConnectionURL", s"jdbc:derby:;databaseName=$metastore;create=true")
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    conf
      .set(KyuubiConf.ENGINE_DEPLOY_YARN_MODE_MEMORY, Math.min(getYarnMaximumAllocationMb, 1024))
      .set(KyuubiConf.ENGINE_DEPLOY_YARN_MODE_CORES, 1)
  }

  override protected def jdbcUrl: String = getJdbcUrl
}
