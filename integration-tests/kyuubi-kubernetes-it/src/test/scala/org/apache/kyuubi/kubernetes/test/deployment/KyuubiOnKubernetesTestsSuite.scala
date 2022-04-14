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

package org.apache.kyuubi.kubernetes.test.deployment

import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.kubernetes.test.WithKyuubiServerOnKubernetes
import org.apache.kyuubi.operation.SparkQueryTests

/**
 * This test is for Kyuubi Server on Kubernetes with Spark engine:
 *
 *   Real World                              Kubernetes Pod
 *  ------------         -----------------------------------------------------
 *  |          |  JDBC   |                                                   |
 *  |  Client  |  ---->  |  Kyuubi Server  ---->  Spark Engine (local mode)  |
 *  |          |         |                                                   |
 *  ------------         -----------------------------------------------------
 */
class KyuubiOnKubernetesTestsSuite extends WithKyuubiServerOnKubernetes with SparkQueryTests {
  override protected val connectionConf: Map[String, String] = Map(
    "spark.master" -> "local",
    "spark.executor.instances" -> "1")

  override protected def jdbcUrl: String = getJdbcUrl
}
