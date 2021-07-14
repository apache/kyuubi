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

package org.apache.kyuubi.ha.v2.engine

import org.apache.curator.framework.CuratorFramework

import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.ha.v2.{InstanceProvider, ProviderStrategy, ServiceDiscoveryClient, ServiceInstance}
import org.apache.kyuubi.ha.v2.ServiceDiscoveryConf._
import org.apache.kyuubi.ha.v2.engine.strategies.SessionNumStrategy
import org.apache.kyuubi.ha.v2.strategies.RandomStrategy

class EngineServiceDiscoveryClient(provider: InstanceProvider[EngineInstance],
  providerStrategy: ProviderStrategy[EngineInstance]) extends ServiceDiscoveryClient {

  override def getInstance(): Option[ServiceInstance] = {
    providerStrategy.getInstance(provider)
      .map(e => ServiceInstance(e.host, e.port))
  }

}


object EngineServiceDiscoveryClient {

  def apply(provider: InstanceProvider[EngineInstance],
            providerStrategy: ProviderStrategy[EngineInstance]): EngineServiceDiscoveryClient =
    new EngineServiceDiscoveryClient(provider, providerStrategy)

  def get(namespace: String, zkClient: CuratorFramework,
          conf: KyuubiConf): EngineServiceDiscoveryClient = {

    val strategy = conf.get(ENGINE_PROVIDER_STRATEGY).toLowerCase match {
      case "random" => RandomStrategy[EngineInstance]()
      case "session_num" => SessionNumStrategy()
      case _ => RandomStrategy[EngineInstance]()
    }

    val tags: Seq[String] = conf.get(ENGINE_PROVIDER_TAGS)
      .map(t => t.split(",").toSeq)
      .getOrElse(Seq())

    val version: Option[String] = conf.get(ENGINE_PROVIDER_VERSION)

    val provider = EngineProvider.builder()
      .namespace(namespace)
      .zkClient(zkClient)
      .version(version)
      .tags(tags)
      .build()

    EngineServiceDiscoveryClient(provider, strategy)
  }

}
