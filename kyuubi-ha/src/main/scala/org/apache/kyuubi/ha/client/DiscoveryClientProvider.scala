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

import java.io.IOException

import org.apache.kyuubi.{KyuubiException, Logging}
import org.apache.kyuubi.config.KyuubiConf

object DiscoveryClientProvider extends Logging {

  /**
   * Creates a zookeeper client before calling `f` and close it after calling `f`.
   */
  def withDiscoveryClient[T](conf: KyuubiConf)(f: DiscoveryClient => T): T = {
    val discoveryClient = createDiscoveryClient(conf)
    try {
      discoveryClient.createClient()
      f(discoveryClient)
    } finally {
      try {
        discoveryClient.closeClient()
      } catch {
        case e: IOException => error("Failed to release the zkClient", e)
      }
    }
  }

  def createDiscoveryClient(conf: KyuubiConf): DiscoveryClient = {
    val classLoader = Thread.currentThread.getContextClassLoader
    val className = conf.get(KyuubiConf.DISCOVERY_CLIENT_CLASS)
    if (className.isEmpty) {
      throw new KyuubiException(
        s"${KyuubiConf.DISCOVERY_CLIENT_CLASS.key} cannot be empty.")
    }
    val cls = Class.forName(className, true, classLoader)
    cls match {
      case c if classOf[DiscoveryClient].isAssignableFrom(cls) =>
        KyuubiConf.createInstance[DiscoveryClient](c, conf)
      case _ => throw new KyuubiException(
        s"$className must extend of ${DiscoveryClient.getClass.getName}")
    }
  }
}
