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

package org.apache.kyuubi.engine.spark

import org.apache.spark.sql.connect.KyuubiSparkConnectService
import org.apache.spark.sql.connect.service.SparkConnectService

import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.ha.client.{EngineServiceDiscovery, ServiceDiscovery}
import org.apache.kyuubi.service.{AbstractFrontendService, Serverable, Service}

class SparkGrpcFrontendService(
    override val serverable: Serverable)
  extends AbstractFrontendService("SparkGrpcFrontend") {

  private lazy val sc = be.asInstanceOf[SparkGrpcBackendService].sparkSession.sparkContext

  var _connectionUrl: String = _

  /**
   * The connection url for client to connect
   */
  override def connectionUrl: String = _connectionUrl

  override lazy val discoveryService: Option[Service] = {
    assert(ServiceDiscovery.supportServiceDiscovery(conf))
    Some(new EngineServiceDiscovery(this))
  }

  override def initialize(conf: KyuubiConf): Unit = {
    this.conf = conf
    SparkConnectService.start(sc)
    val (host, port) = KyuubiSparkConnectService.hostAndPort
    _connectionUrl = s"$host:$port"
    super.initialize(conf)
  }
}
