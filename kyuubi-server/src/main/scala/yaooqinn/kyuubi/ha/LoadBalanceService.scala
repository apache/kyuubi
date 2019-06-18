/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package yaooqinn.kyuubi.ha

import yaooqinn.kyuubi.Logging
import yaooqinn.kyuubi.server.KyuubiServer

/**
 * An implementation of High Availability service in load balance mode. In this mode, all Kyuubi
 * service instance URI will be published in root service name as soon as the server starts.
 */
private[kyuubi] class LoadBalanceService private(name: String, server: KyuubiServer)
  extends HighAvailableService(name, server) with Logging{

  def this(server: KyuubiServer) = {
    this(classOf[LoadBalanceService].getSimpleName, server)
  }

  override def start(): Unit = {
    publishService()
    super.start()
  }

  override def stop(): Unit = {
    offlineService()
    Option(zkClient).foreach(_.close())
    super.stop()
  }

  override def reset(): Unit = {
    server.deregisterWithZK()
    // If there are no more active client sessions, stop the server
    if (server.beService.getSessionManager.getOpenSessionCount == 0) {
      warn("This Kyuubi instance has been removed from the list of " +
        "server instances available for dynamic service discovery. The last client " +
        "session has ended - will shutdown now.")
      server.stop()
    }
  }
}
