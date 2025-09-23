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

package org.apache.kyuubi.plugin.spark.authz

import com.dimafeng.testcontainers.MySQLContainer
import org.testcontainers.utility.DockerImageName

trait MysqlContainerEnv {

  val containerDef: MySQLContainer = MySQLContainer.Def(
    dockerImageName = DockerImageName.parse("mysql:8"),
    databaseName = "hive_metastore",
    username = "root",
    password = "123456")
    .createContainer()

  def startEngine(): Unit = {
    containerDef.start()
  }

  def stopEngine(): Unit = {
    containerDef.stop()
  }
}
