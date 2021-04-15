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

package org.apache.kyuubi.operation

import org.apache.kyuubi.{Utils, WithKyuubiServer, WithMiniYarnCluster}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf.ENGINE_INIT_TIMEOUT
import org.apache.kyuubi.config.internal.Tests.TESTING_HADOOP_CONF_DIR

class KyuubiOperationYarnClusterSuite extends WithKyuubiServer with WithMiniYarnCluster
  with JDBCTestUtils {
  override protected def jdbcUrl: String = getJdbcUrl

  override val hadoopConfDir = Utils.createTempDir().toFile

  override protected val conf: KyuubiConf = {
    KyuubiConf().set(ENGINE_INIT_TIMEOUT, 300000L)
      .set("spark.master", "yarn")
      .set(TESTING_HADOOP_CONF_DIR, hadoopConfDir.getAbsolutePath)
      .setIfMissing("spark.executor.instances", "1")
  }

  test("KYUUBI #527- Support test with mini yarn cluster") {
    withJdbcStatement() { statement =>
      val resultSet = statement.executeQuery("SELECT 'yarn' AS master")
      assert(resultSet.next())
      assert(resultSet.getString("master") === "yarn")
    }
  }
}
