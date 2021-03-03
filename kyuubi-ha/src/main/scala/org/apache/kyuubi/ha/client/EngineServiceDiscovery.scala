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

package org.apache.kyuubi.ha.client

import scala.util.control.NonFatal

import org.apache.kyuubi.config.KyuubiConf.ENGINE_SHARED_LEVEL
import org.apache.kyuubi.service.Serverable

/**
 * A service for service discovery used by engine side.
 *
 * @param name the name of the service itself
 * @param server the instance uri a service that used to publish itself
 */
class EngineServiceDiscovery private(
    name: String,
    server: Serverable) extends ServiceDiscovery(server) {
  def this(server: Serverable) =
    this(classOf[EngineServiceDiscovery].getSimpleName, server)

  override def stop(): Unit = {
    conf.get(ENGINE_SHARED_LEVEL) match {
      // For connection level, we should clean up the namespace in zk in case the disk stress.
      case "CONNECTION" =>
        cleanup()
        info("Clean up discovery service due to this is connection share level.")

      case _ =>
    }
    super.stop()
  }

  private def cleanup(): Unit = {
    if (namespace != null) {
      try {
        zkClient.delete().deletingChildrenIfNeeded().forPath(namespace)
      } catch {
        case NonFatal(e) =>
          warn("Failed to clean up Spark engine before stop.", e)
      }
    }
  }
}


