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

package org.apache.kyuubi.ha.client.zookeeper

import java.io.IOException
import java.nio.charset.StandardCharsets
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean

import scala.collection.JavaConverters._

import com.google.common.annotations.VisibleForTesting

import org.apache.kyuubi.{KYUUBI_VERSION, KyuubiException, KyuubiSQLException, Logging}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiReservedKeys.KYUUBI_ENGINE_ID
import org.apache.kyuubi.ha.HighAvailabilityConf.{HA_ENGINE_REF_ID, HA_ZK_NODE_TIMEOUT, HA_ZK_PUBLISH_CONFIGS}
import org.apache.kyuubi.ha.client.{DiscoveryClient, ServiceDiscovery, ServiceNodeInfo}
import org.apache.kyuubi.ha.client.zookeeper.ZookeeperClientProvider.{buildZookeeperClient, getGracefulStopThreadDelay}
import org.apache.kyuubi.ha.client.zookeeper.ZookeeperDiscoveryClient.connectionChecker
import org.apache.kyuubi.shaded.curator.framework.CuratorFramework
import org.apache.kyuubi.shaded.curator.framework.recipes.atomic.{AtomicValue, DistributedAtomicInteger}
import org.apache.kyuubi.shaded.curator.framework.recipes.locks.InterProcessSemaphoreMutex
import org.apache.kyuubi.shaded.curator.framework.recipes.nodes.PersistentNode
import org.apache.kyuubi.shaded.curator.framework.state.{ConnectionState, ConnectionStateListener}
import org.apache.kyuubi.shaded.curator.framework.state.ConnectionState.{CONNECTED, LOST, RECONNECTED}
import org.apache.kyuubi.shaded.curator.retry.RetryForever
import org.apache.kyuubi.shaded.curator.utils.ZKPaths
import org.apache.kyuubi.shaded.zookeeper.{CreateMode, KeeperException, WatchedEvent, Watcher}
import org.apache.kyuubi.shaded.zookeeper.CreateMode.PERSISTENT
import org.apache.kyuubi.shaded.zookeeper.KeeperException.NodeExistsException
import org.apache.kyuubi.util.ThreadUtils

class ZookeeperDiscoveryClient(conf: KyuubiConf) extends DiscoveryClient {

  private val zkClient: CuratorFramework = buildZookeeperClient(conf)
  @volatile private var serviceNode: PersistentNode = _
  private var watcher: DeRegisterWatcher = _

  override def createClient(): Unit = {
    zkClient.start()
  }

  override def closeClient(): Unit = {
    if (zkClient != null) {
      zkClient.close()
    }
  }

  override def create(path: String, mode: String, createParent: Boolean = true): String = {
    val builder =
      if (createParent) zkClient.create().creatingParentsIfNeeded() else zkClient.create()
    builder
      .withMode(CreateMode.valueOf(mode))
      .forPath(path)
  }

  override def getData(path: String): Array[Byte] = {
    zkClient.getData.forPath(path)
  }

  override def setData(path: String, data: Array[Byte]): Boolean = {
    zkClient.setData().forPath(path, data) != null
  }

  override def getChildren(path: String): List[String] = {
    zkClient.getChildren.forPath(path).asScala.toList
  }

  override def pathExists(path: String): Boolean = {
    zkClient.checkExists().forPath(path) != null
  }

  override def pathNonExists(path: String, isPrefix: Boolean): Boolean = {
    zkClient.checkExists().forPath(path) == null
  }

  override def delete(path: String, deleteChildren: Boolean = false): Unit = {
    if (deleteChildren) {
      zkClient.delete().deletingChildrenIfNeeded().forPath(path)
    } else {
      zkClient.delete().forPath(path)
    }
  }

  override def monitorState(serviceDiscovery: ServiceDiscovery): Unit = {
    zkClient
      .getConnectionStateListenable.addListener(new ConnectionStateListener {
        private val isConnected = new AtomicBoolean(false)

        override def stateChanged(client: CuratorFramework, newState: ConnectionState): Unit = {
          info(s"Zookeeper client connection state changed to: $newState")
          newState match {
            case CONNECTED | RECONNECTED => isConnected.set(true)
            case LOST =>
              isConnected.set(false)
              val delay = getGracefulStopThreadDelay(conf)
              connectionChecker.schedule(
                new Runnable {
                  override def run(): Unit = if (!isConnected.get()) {
                    error(s"Zookeeper client connection state changed to: $newState," +
                      s" but failed to reconnect in ${delay / 1000} seconds." +
                      s" Give up retry and stop gracefully . ")
                    serviceDiscovery.stopGracefully(true)
                  }
                },
                delay,
                TimeUnit.MILLISECONDS)
            case _ =>
          }
        }
      })
  }

  override def tryWithLock[T](lockPath: String, timeout: Long)(f: => T): T = {
    var lock: InterProcessSemaphoreMutex = null
    try {
      try {
        lock = new InterProcessSemaphoreMutex(zkClient, lockPath)
        // Acquire a lease. If no leases are available, this method blocks until either the
        // maximum number of leases is increased or another client/process closes a lease.
        //
        // Here, we should throw exception if timeout during acquiring lock.
        // Let's say we have three clients with same request lock to two kyuubi server instances.
        //
        //  client A  --->  kyuubi X  -- first acquired  \
        //  client B  --->  kyuubi X  -- second acquired --  zookeeper
        //  client C  --->  kyuubi Y  -- third acquired  /
        //
        // The first client A acqiured the lock then B and C are blocked until A release the lock,
        // with the A created engine state:
        //   - SUCCESS
        //     B acquired the lock then get engine ref and release the lock.
        //     C acquired the lock then get engine ref and release the lock.
        //   - FAILED or TIMEOUT
        //     B acquired the lock then try to create engine again if not timeout.
        //     C should be timeout and throw exception back to client. This fast fail
        //     to avoid client too long to waiting in concurrent.

        // Return false means we are timeout
        val acquired = lock.acquire(timeout, TimeUnit.MILLISECONDS)
        if (!acquired) {
          throw KyuubiSQLException(s"Timeout to lock on path [$lockPath] after " +
            s"$timeout ms. There would be some problem that other session may " +
            s"create engine timeout.")
        }
      } catch {
        case e: KyuubiSQLException => throw e
        case e: Exception => throw KyuubiSQLException(s"Lock failed on path [$lockPath]", e)
      }
      f
    } finally {
      try {
        if (lock != null) {
          lock.release()
        }
      } catch {
        case _: Exception =>
      }
    }
  }

  override def getServerHost(namespace: String): Option[(String, Int)] = {
    // TODO: use last one because to avoid touching some maybe-crashed engines
    // We need a big improvement here.
    getServiceNodesInfo(namespace, Some(1), silent = true) match {
      case Seq(sn) => Some((sn.host, sn.port))
      case _ => None
    }
  }

  override def getEngineByRefId(
      namespace: String,
      engineRefId: String): Option[(String, Int)] = {
    getServiceNodesInfo(namespace, silent = true)
      .find(_.engineRefId.exists(_.equals(engineRefId)))
      .map(data => (data.host, data.port))
  }

  override def getServiceNodesInfo(
      namespace: String,
      sizeOpt: Option[Int] = None,
      silent: Boolean = false): Seq[ServiceNodeInfo] = {
    try {
      val hosts = zkClient.getChildren.forPath(namespace)
      val size = sizeOpt.getOrElse(hosts.size())
      hosts.asScala.takeRight(size).map { p =>
        val path = ZKPaths.makePath(namespace, p)
        val instance = new String(zkClient.getData.forPath(path), StandardCharsets.UTF_8)
        val (host, port) = DiscoveryClient.parseInstanceHostPort(instance)
        val attributes =
          p.split(";").map(_.split("=", 2)).filter(_.length == 2).map(kv =>
            (kv.head, kv.last)).toMap
        val version = attributes.get("version")
        val engineRefId = attributes.get("refId")
        val engineIdStr = attributes.get(KYUUBI_ENGINE_ID).map(" engine id:" + _).getOrElse("")
        info(s"Get service instance:$instance$engineIdStr and version:${version.getOrElse("")} " +
          s"under $namespace")
        ServiceNodeInfo(namespace, p, host, port, version, engineRefId, attributes)
      }.toSeq
    } catch {
      case _: Exception if silent => Nil
      case e: Exception =>
        error(s"Failed to get service node info", e)
        Nil
    }
  }

  override def registerService(
      conf: KyuubiConf,
      namespace: String,
      serviceDiscovery: ServiceDiscovery,
      version: Option[String] = None,
      external: Boolean = false): Unit = {
    val instance = serviceDiscovery.fe.connectionUrl
    watcher = new DeRegisterWatcher(instance, serviceDiscovery)
    serviceNode = createPersistentNode(
      conf,
      namespace,
      instance,
      version,
      external,
      serviceDiscovery.fe.attributes)
    // Set a watch on the serviceNode
    watchNode()
  }

  override def deregisterService(): Unit = {
    // close the EPHEMERAL_SEQUENTIAL node in zk
    if (serviceNode != null) {
      try {
        serviceNode.close()
      } catch {
        case e @ (_: IOException | _: KeeperException) =>
          error("Failed to close the persistent ephemeral znode" + serviceNode.getActualPath, e)
      } finally {
        serviceNode = null
      }
    }
  }

  override def postDeregisterService(namespace: String): Boolean = {
    if (namespace != null) {
      try {
        delete(namespace, true)
        true
      } catch {
        case e: KeeperException =>
          warn(s"Failed to delete $namespace", e)
          false
      }
    } else {
      false
    }
  }

  override def createAndGetServiceNode(
      conf: KyuubiConf,
      namespace: String,
      instance: String,
      version: Option[String] = None,
      external: Boolean = false): String = {
    createPersistentNode(conf, namespace, instance, version, external).getActualPath
  }

  @VisibleForTesting
  override def startSecretNode(
      createMode: String,
      basePath: String,
      initData: String,
      useProtection: Boolean = false): Unit = {
    val secretNode = new PersistentNode(
      zkClient,
      CreateMode.valueOf(createMode),
      useProtection,
      basePath,
      initData.getBytes(StandardCharsets.UTF_8))
    secretNode.start()
    val znodeTimeout = conf.get(HA_ZK_NODE_TIMEOUT)
    if (!secretNode.waitForInitialCreate(znodeTimeout, TimeUnit.MILLISECONDS)) {
      throw new KyuubiException(s"Max znode creation wait time $znodeTimeout s exhausted")
    }
  }

  override def getAndIncrement(path: String, delta: Int = 1): Int = {
    val dai = new DistributedAtomicInteger(zkClient, path, new RetryForever(1000))
    var atomicVal: AtomicValue[Integer] = null
    do {
      atomicVal = dai.add(delta)
    } while (atomicVal == null || !atomicVal.succeeded())
    atomicVal.preValue().intValue()
  }

  /**
   * Refer to the implementation of HIVE-11581 to simplify user connection parameters.
   * https://issues.apache.org/jira/browse/HIVE-11581
   * HiveServer2 should store connection params in ZK
   * when using dynamic service discovery for simpler client connection string.
   */
  private[client] def addConfsToPublish(conf: KyuubiConf, instance: String): String = {
    if (!instance.contains(":")) {
      return instance
    }
    val hostPort = instance.split(":", 2)
    val confsToPublish = collection.mutable.Map[String, String]()

    // Hostname
    confsToPublish += ("hive.server2.thrift.bind.host" -> hostPort(0))
    // Transport mode
    confsToPublish += ("hive.server2.transport.mode" -> "binary")
    // Transport specific confs
    confsToPublish += ("hive.server2.thrift.port" -> hostPort(1))
    confsToPublish += ("hive.server2.thrift.sasl.qop" -> conf.get(KyuubiConf.SASL_QOP))
    // Auth specific confs
    val authenticationMethod = conf.get(KyuubiConf.AUTHENTICATION_METHOD).mkString(",")
    confsToPublish += ("hive.server2.authentication" -> authenticationMethod)
    if (authenticationMethod.equalsIgnoreCase("KERBEROS")) {
      confsToPublish += ("hive.server2.authentication.kerberos.principal" ->
        conf.get(KyuubiConf.SERVER_PRINCIPAL).getOrElse(""))
    }
    confsToPublish.map { case (k, v) => k + "=" + v }.mkString(";")
  }

  private def createPersistentNode(
      conf: KyuubiConf,
      namespace: String,
      instance: String,
      version: Option[String] = None,
      external: Boolean = false,
      attributes: Map[String, String] = Map.empty): PersistentNode = {
    val ns = ZKPaths.makePath(null, namespace)
    try {
      zkClient
        .create()
        .creatingParentsIfNeeded()
        .withMode(PERSISTENT)
        .forPath(ns)
    } catch {
      case _: NodeExistsException => // do nothing
      case e: KeeperException =>
        throw new KyuubiException(s"Failed to create namespace '$ns'", e)
    }

    val session = conf.get(HA_ENGINE_REF_ID)
      .map(refId => s"refId=$refId;").getOrElse("")
    val extraInfo = attributes.map(kv => kv._1 + "=" + kv._2).mkString(";", ";", "")
    val pathPrefix = ZKPaths.makePath(
      namespace,
      s"serverUri=$instance;version=${version.getOrElse(KYUUBI_VERSION)}" +
        s"${extraInfo.stripSuffix(";")};${session}sequence=")
    var localServiceNode: PersistentNode = null
    val createMode =
      if (external) CreateMode.PERSISTENT_SEQUENTIAL
      else CreateMode.EPHEMERAL_SEQUENTIAL
    val znodeData =
      if (conf.get(HA_ZK_PUBLISH_CONFIGS) && session.isEmpty) {
        addConfsToPublish(conf, instance)
      } else {
        instance
      }
    try {
      localServiceNode = new PersistentNode(
        zkClient,
        createMode,
        false,
        pathPrefix,
        znodeData.getBytes(StandardCharsets.UTF_8))
      localServiceNode.start()
      val znodeTimeout = conf.get(HA_ZK_NODE_TIMEOUT)
      if (!localServiceNode.waitForInitialCreate(znodeTimeout, TimeUnit.MILLISECONDS)) {
        throw new KyuubiException(s"Max znode creation wait time $znodeTimeout s exhausted")
      }
      info(s"Created a ${localServiceNode.getActualPath} on ZooKeeper for KyuubiServer uri:" +
        s" $instance")
    } catch {
      case e: Exception =>
        if (localServiceNode != null) {
          localServiceNode.close()
        }
        throw new KyuubiException(
          s"Unable to create a znode for this server instance: $instance",
          e)
    }
    localServiceNode
  }

  private def watchNode(): Unit = {
    if (zkClient.checkExists
        .usingWatcher(watcher.asInstanceOf[Watcher]).forPath(serviceNode.getActualPath) == null) {
      // No node exists, throw exception
      throw new KyuubiException(s"Unable to create znode for this Kyuubi " +
        s"instance[${watcher.instance}] on ZooKeeper.")
    }
  }

  class DeRegisterWatcher(val instance: String, serviceDiscovery: ServiceDiscovery)
    extends Watcher {
    override def process(event: WatchedEvent): Unit = {
      if (event.getType == Watcher.Event.EventType.NodeDeleted) {
        warn(s"This Kyuubi instance $instance is now de-registered from" +
          " ZooKeeper. The server will be shut down after the last client session completes.")
        ZookeeperDiscoveryClient.this.deregisterService()
        serviceDiscovery.stopGracefully()
      } else if (event.getType == Watcher.Event.EventType.NodeDataChanged) {
        warn(s"This Kyuubi instance $instance now receives the NodeDataChanged event")
        ZookeeperDiscoveryClient.this.watchNode()
      }
    }
  }
}

object ZookeeperDiscoveryClient extends Logging {
  final private lazy val connectionChecker =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor("zk-connection-checker")
}
