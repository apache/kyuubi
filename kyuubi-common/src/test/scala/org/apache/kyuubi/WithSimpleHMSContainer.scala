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

package org.apache.kyuubi

import com.dimafeng.testcontainers.{ContainerDef, GenericContainer}
import com.dimafeng.testcontainers.scalatest.TestContainerForAll
import org.testcontainers.containers.wait.strategy.HostPortWaitStrategy

trait WithSimpleHMSContainer extends KyuubiFunSuite with TestContainerForAll {

  final val DOCKER_IMAGE_NAME = "nekyuubi/kyuubi-hive-metastore:latest"

  private val exposedHmsPort = 9083

  private var _hmsThriftUris: String = _

  def hmsThriftUris: String = _hmsThriftUris

  override val containerDef: SimpleHMSContainer.Def =
    SimpleHMSContainer.Def(DOCKER_IMAGE_NAME, exposedHmsPort)

  override def afterContainersStart(containers: Containers): Unit = {
    _hmsThriftUris = "thrift://localhost:" + containers.mappedPort(exposedHmsPort)
  }
}

object SimpleHMSContainer {
  case class Def(
      dockerImage: String,
      exposedHmsPort: Int,
      env: Map[String, String] = Map())
    extends ContainerDef {

    override type Container = GenericContainer

    override def createContainer(): Container = new GenericContainer(
      GenericContainer(
        dockerImage,
        exposedPorts = Seq(exposedHmsPort),
        env = env,
        waitStrategy = new HostPortWaitStrategy().forPorts(exposedHmsPort)))
  }
}
