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

import org.apache.kyuubi.config.KyuubiConf.ENGINE_SHARE_LEVEL
import org.apache.kyuubi.service.FrontendService

/**
 * A service for service discovery used by engine side.
 *
 * @param fe the frontend service to publish for service discovery
 */
class EngineServiceDiscovery(
    fe: FrontendService) extends ServiceDiscovery("EngineServiceDiscovery", fe) {

  override def stop(): Unit = synchronized {
    closeServiceNode()
    conf.get(ENGINE_SHARE_LEVEL) match {
      // For connection level, we should clean up the namespace in zk in case the disk stress.
      case "CONNECTION" if namespace != null =>
        try {
          zkClient.delete().deletingChildrenIfNeeded().forPath(namespace)
          info("Clean up discovery service due to this is connection share level.")
        } catch {
          case NonFatal(e) =>
            warn("Failed to clean up Spark engine before stop.", e)
        }

      case _ =>
    }
    super.stop()
  }
}
