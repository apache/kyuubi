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

package org.apache.kyuubi.spark.connector.kudu

import java.io.File

import com.dimafeng.testcontainers.{DockerComposeContainer, ExposedService, ForAllTestContainer}

import org.apache.kyuubi.{KyuubiFunSuite, Utils}

trait KuduMixin extends KyuubiFunSuite with ForAllTestContainer {

  private val KUDU_MASTER_PORT = 7051

  override val container: DockerComposeContainer =
    DockerComposeContainer
      .Def(
        composeFiles =
          new File(Utils.getContextOrKyuubiClassLoader.getResource("kudu-compose.yml").toURI),
        exposedServices = ExposedService("kudu-master", KUDU_MASTER_PORT) :: Nil)
      .createContainer()

  def kuduMasterHost: String = container.getServiceHost("kudu-master", KUDU_MASTER_PORT)
  def kuduMasterPort: Int = container.getServicePort("kudu-master", KUDU_MASTER_PORT)
  def kuduMasterUrl: String = s"$kuduMasterHost:$kuduMasterPort"
}
