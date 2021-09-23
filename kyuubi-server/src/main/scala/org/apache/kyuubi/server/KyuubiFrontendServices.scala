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

package org.apache.kyuubi.server

import org.apache.kyuubi.Logging
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.service.{AbstractFrontendService, BackendService, CompositeService, ServiceState, ThriftFrontendService}

/**
 * A kyuubi frontend service is a kind of composite service, which used to
 * composite multiple frontend services for kyuubi server.
 */
class KyuubiFrontendServices private(name: String, be: BackendService, oomHook: Runnable)
  extends CompositeService(name) with Logging {

  def this(be: BackendService, oomHook: Runnable) = {
    this(classOf[KyuubiFrontendServices].getSimpleName, be, oomHook)
  }

  def connectionUrl(server: Boolean): String = {
    getServiceState match {
      case s @ ServiceState.LATENT => throw new IllegalStateException(s"Illegal Service State: $s")
      case _ =>
        val defaultFEService = getServices(0).asInstanceOf[AbstractFrontendService]
        if (defaultFEService != null && defaultFEService.isInstanceOf[ThriftFrontendService]) {
          defaultFEService.connectionUrl(server)
        } else {
          throw new IllegalStateException("Can not find thrift frontend services!")
        }
    }
  }

  override def initialize(conf: KyuubiConf): Unit = {
    addService(new ThriftFrontendService(be, oomHook))
    super.initialize(conf)
  }
}
