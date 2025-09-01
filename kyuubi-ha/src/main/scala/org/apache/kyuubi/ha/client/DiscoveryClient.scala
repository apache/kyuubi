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

import org.apache.kyuubi.Logging
import org.apache.kyuubi.config.KyuubiConf

/**
 * A collection of apis that discovery client need implement.
 */
trait DiscoveryClient extends Logging {

  /**
   * Create a discovery client.
   */
  def createClient(): Unit

  /**
   * Close the discovery client.
   */
  def closeClient(): Unit

  /**
   * Create path on discovery service.
   */
  def create(path: String, mode: String, createParent: Boolean = true): String

  /**
   * Get the stored data under path.
   */
  def getData(path: String): Array[Byte]

  /**
   * Set the data under path.
   */
  def setData(path: String, data: Array[Byte]): Boolean

  /**
   * Get the paths under given path.
   * @return list of path
   */
  def getChildren(path: String): List[String]

  /**
   * Check if the path is exists.
   */
  def pathExists(path: String): Boolean

  /**
   * Checks whether the given path and all its child paths (prefix matches) do not exist.
   * The isPrefix is set to true by default for Etcd to retrieve all entries under the given path.
   * For other discovery service, it can be set false by default to only check the exact path.
   * @param path The path to check
   * @return true if the path and all its sub-paths are non-existent; false otherwise
   */
  def pathNonExists(path: String, isPrefix: Boolean = false): Boolean

  /**
   * Delete a path.
   * @param path the path to be deleted
   * @param deleteChildren if true, will also delete children if they exist.
   */
  def delete(path: String, deleteChildren: Boolean = false): Unit

  /**
   * Add a monitor for serviceDiscovery. It is used to stop service discovery gracefully
   * when disconnect.
   */
  def monitorState(serviceDiscovery: ServiceDiscovery): Unit

  /**
   * The distributed lock path used to ensure only once engine being created for non-CONNECTION
   * share level.
   * @param timeout the timeout of acquiring lock, unit is ms
   * @throws KyuubiSQLException if timeout or get any exception during acquiring lock
   */
  def tryWithLock[T](lockPath: String, timeout: Long)(f: => T): T

  /**
   * Get the engine address and port from engine space.
   * @return engine host and port
   */
  def getServerHost(namespace: String): Option[(String, Int)]

  /**
   * Get engine info by engine ref id from engine space.
   * @param namespace the path to get engine ref
   * @param engineRefId engine ref id
   * @return engine host and port
   */
  def getEngineByRefId(
      namespace: String,
      engineRefId: String): Option[(String, Int)]

  /**
   * Get service node info from server space.
   * @param namespace the path to get node info
   * @param sizeOpt how many nodes to pick
   * @param silent if true, error message will not be logged
   * @return Service node info
   */
  def getServiceNodesInfo(
      namespace: String,
      sizeOpt: Option[Int] = None,
      silent: Boolean = false): Seq[ServiceNodeInfo]

  /**
   * Register Kyuubi instance on discovery service.
   * @param conf Kyuubi config
   * @param namespace the path to register instance
   * @param serviceDiscovery service discovery
   * @param version kyuubi version
   * @param external if true,
   *                 the service info will not be automatically deleted upon client's disconnect
   */
  def registerService(
      conf: KyuubiConf,
      namespace: String,
      serviceDiscovery: ServiceDiscovery,
      version: Option[String] = None,
      external: Boolean = false): Unit

  /**
   * Deregister Kyuubi instance on discovery service.
   */
  def deregisterService(): Unit

  /**
   * Request remove Kyuubi instance on discovery service.
   */
  def postDeregisterService(namespace: String): Boolean

  /**
   * Create server service node info on discovery and get the actual path.
   * @param conf Kyuubi config
   * @param namespace the path to register instance
   * @param instance server info, host:port
   * @param version kyuubi version
   * @param external if true,
   *                 the service info will not be automatically deleted upon client's disconnect
   */
  def createAndGetServiceNode(
      conf: KyuubiConf,
      namespace: String,
      instance: String,
      version: Option[String] = None,
      external: Boolean = false): String

  /**
   * Create a node to store engine secret.
   * @param createMode create node mode, automatically deleted or not
   * @param basePath the base path for the node
   * @param initData the init data to be stored
   * @param useProtection if true, createBuilder with protection
   */
  def startSecretNode(
      createMode: String,
      basePath: String,
      initData: String,
      useProtection: Boolean = false): Unit

  /**
   * Atomically get an Int number and add one
   * @param path the path of stored data,
   *             If the path does not exist, it will be created and initialized to 0
   * @param delta the increase num
   * @return the stored data under path
   */
  def getAndIncrement(path: String, delta: Int = 1): Int
}

object DiscoveryClient {

  /**
   * Parse instance info string, get host and port.
   */
  private[client] def parseInstanceHostPort(instance: String): (String, Int) = {
    val maybeInfos = instance.split(";")
      .map(_.split("=", 2))
      .filter(_.length == 2)
      .map(i => (i(0), i(1)))
      .toMap
    if (maybeInfos.nonEmpty) {
      (
        maybeInfos("hive.server2.thrift.bind.host"),
        maybeInfos("hive.server2.thrift.port").toInt)
    } else {
      val strings = instance.split(":")
      (strings(0), strings(1).toInt)
    }
  }
}
