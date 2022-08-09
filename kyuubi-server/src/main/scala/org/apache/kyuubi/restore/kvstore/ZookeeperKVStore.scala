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
package org.apache.kyuubi.restore.kvstore

import org.apache.curator.framework.CuratorFramework
import org.apache.curator.utils.ZKPaths

import org.apache.kyuubi.Logging
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.ha.client.zookeeper.ZookeeperClientProvider
import org.apache.kyuubi.restore.kvstore.ZookeeperKVStore._

class ZookeeperKVStore(conf: KyuubiConf) extends KVStore with Logging {

  private val zkClient: CuratorFramework = buildZookeeperClient(conf)

  override def get[T](key: String, klass: Class[T]): Option[T] = {
    val data = zkClient.getData()
      .forPath(getPath(key))
    if (data != null) {
      try {
        Some(serializer.deserialize[T](data, klass))
      } catch {
        case e: Exception =>
          warn(s"Deserialize value failed, key: [$key].", e)
          None
      }
    } else {
      None
    }
  }

  override def set(key: String, value: Any): Unit = {
    // TODO TTL
    val data = serializer.serialize(value)
    zkClient.create()
      .creatingParentsIfNeeded()
      .forPath(getPath(key), data)
  }

  override def remove(key: String): Unit = {
    zkClient.delete()
      .forPath(getPath(key))
  }

  // TODO add conf entry and subdirectories of kyuubi.ha.namespace
  private val storeNamespace = conf.getOption("kyuubi.store.namespace").getOrElse("/kyuubi_store")
  private def getPath(key: String): String = {
    ZKPaths.makePath(storeNamespace, key)
  }
}

object ZookeeperKVStore {

  def apply(conf: KyuubiConf): ZookeeperKVStore = new ZookeeperKVStore(conf)

  private def buildZookeeperClient(conf: KyuubiConf): CuratorFramework = {
    // TODO stripped from kyuubi-ha
    val zkClient = ZookeeperClientProvider.buildZookeeperClient(conf)
    zkClient.start()
    zkClient
  }

  private val serializer: KVStoreSerializer = new KVStoreSerializer()

}
