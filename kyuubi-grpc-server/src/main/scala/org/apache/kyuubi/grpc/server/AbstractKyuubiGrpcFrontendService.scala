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

package org.apache.kyuubi.grpc.server

import io.grpc.{Channel, Grpc, InsecureChannelCredentials, ManagedChannel}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.grpc.service.AbstractGrpcFrontendService

abstract class AbstractKyuubiGrpcFrontendService(name: String)
  extends AbstractGrpcFrontendService(name) {

  protected var host = ""
  protected var port = 0

  def channel: ManagedChannel

  def startEngine(): (String, Int)

  override def initialize(conf: KyuubiConf): Unit = {
    val serverInfo = startEngine()
    host = serverInfo._1
    port = serverInfo._2
    super.initialize(conf)
  }


}
