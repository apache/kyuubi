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
import java.util.concurrent.{ScheduledFuture, TimeUnit}
import java.util.concurrent.atomic.AtomicBoolean

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

import com.google.common.annotations.VisibleForTesting

import org.apache.kyuubi.{KYUUBI_VERSION, KyuubiException, KyuubiSQLException, Logging}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiReservedKeys.KYUUBI_ENGINE_ID
import org.apache.kyuubi.ha.HighAvailabilityConf.{HA_ENGINE_REF_ID, HA_ZK_NODE_TIMEOUT, HA_ZK_PUBLISH_CONFIGS}
import org.apache.kyuubi.ha.client.{DiscoveryClient, ServiceDiscovery, ServiceNodeInfo}
import org.apache.kyuubi.ha.client.zookeeper.ZookeeperClientProvider.{buildZookeeperClient, getGracefulStopThreadDelay}
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
  @volatile private var watcher: DeRegisterWatcher = _

  // Monotonic generation that tags each RECONNECTED rewatch attempt. A retry scheduled for an
  // older generation cancels itself when a newer RECONNECTED arrives, so repeated reconnects
  // during ZK flapping never stack independent retry chains (single-flight).
  private val rewatchGeneration = new java.util.concurrent.atomic.AtomicLong(0L)
  // The currently pending scheduled retry, if any. Cancelled on deregister so a stale task cannot
  // rearm a watcher on a node that is already torn down.
  @volatile private var rewatchRetryTask: ScheduledFuture[_] = _

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
    registerConnectionStateListener(serviceDiscovery)
  }

  @VisibleForTesting
  private[kyuubi] def registerConnectionStateListener(
      serviceDiscovery: ServiceDiscovery): ConnectionStateListener = {
    val listener = new ConnectionStateListener {
      private val isConnected = new AtomicBoolean(false)

      override def stateChanged(client: CuratorFramework, newState: ConnectionState): Unit = {
        info(s"Zookeeper client connection state changed to: $newState")
        newState match {
          case CONNECTED => isConnected.set(true)
          case RECONNECTED =>
            isConnected.set(true)
            // Rearm the DeRegisterWatcher off the connection-state dispatch thread. Curator 2.12
            // dispatches ConnectionStateListener callbacks on a single-thread executor
            // shared with PersistentNode's recovery listener; a foreground checkExists here could
            // block that thread (and delay PersistentNode re-creating the znode and subsequent
            // LOST handling) when the connection drops again mid-call. Schedule the rewatch on a
            // dedicated executor so stateChanged returns immediately.
            scheduleRewatch("Zookeeper client reconnected")
          case LOST =>
            isConnected.set(false)
            val delay = getGracefulStopThreadDelay(conf)
            ZookeeperDiscoveryClient.connectionChecker.schedule(
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
    }
    zkClient.getConnectionStateListenable.addListener(listener)
    listener
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
    // Cancel any pending rewatch retry so a stale task cannot rearm a watcher on a node that has
    // just been torn down. Bump the generation first so an in-flight retry observes the change.
    cancelRewatchRetry()
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
    if (!watchServiceNode("service registration")) {
      // No node exists, throw exception
      throw new KyuubiException(s"Unable to create znode for this Kyuubi " +
        s"instance[${Option(watcher).map(_.instance).getOrElse("unknown")}] on ZooKeeper.")
    }
  }

  private def watchServiceNode(reason: String): Boolean = {
    val currentServiceNode = serviceNode
    val currentWatcher = watcher
    if (currentServiceNode == null || currentWatcher == null) {
      debug(s"Skip setting DeRegisterWatcher for $reason because service node is not registered")
      return false
    }

    val path = currentServiceNode.getActualPath
    if (path == null) {
      debug(s"Skip setting DeRegisterWatcher for $reason because service node path is not ready")
      return false
    }

    val serviceNodeExists = zkClient.checkExists
      .usingWatcher(currentWatcher.asInstanceOf[Watcher])
      .forPath(path) != null
    if (serviceNodeExists) {
      info(s"Set DeRegisterWatcher on $path for $reason")
    } else {
      warn(s"Service node $path does not exist when setting DeRegisterWatcher for $reason")
    }
    serviceNodeExists
  }

  /**
   * Attempt to rearm the DeRegisterWatcher off the connection-state dispatch thread. Runs on a
   * dedicated rewatch executor (isolated from the shared connectionChecker that the LOST graceful
   * stop task may block on). Retries are generation-gated: a newer RECONNECTED bumps the
   * generation, so a retry scheduled for an older generation self-cancels instead of stacking
   * another independent chain.
   *
   * The scheduled task only invokes private methods (not field reads) so that the private
   * serviceNode/watcher fields are not accessed from an anonymous inner class, which would make
   * scalac mangle their names and break reflection-based test setup.
   */
  private def scheduleRewatch(reason: String): Unit = {
    scheduleRewatch(reason, attempt = 1, rewatchGeneration.incrementAndGet())
  }

  private def scheduleRewatch(reason: String, attempt: Int, generation: Long): Unit = {
    rewatchRetryTask = ZookeeperDiscoveryClient.rewatchExecutor.schedule(
      new Runnable {
        override def run(): Unit = runRewatchAttempt(reason, attempt, generation)
      },
      0L,
      TimeUnit.MILLISECONDS)
  }

  private def runRewatchAttempt(reason: String, attempt: Int, generation: Long): Unit = {
    if (!rewatchStillActive(generation)) {
      debug(s"Discard stale rewatch for $reason at attempt $attempt" +
        s" (generation $generation vs ${rewatchGeneration.get()})")
      return
    }
    try {
      if (watchServiceNode(s"$reason (attempt $attempt)")) {
        rewatchRetryTask = null
      } else if (attempt < ZookeeperDiscoveryClient.RewatchNodeMaxRetries &&
        rewatchStillActive(generation)) {
        val nextAttempt = attempt + 1
        warn(s"Failed to rewatch service node for $reason at attempt $attempt, " +
          s"will retry at attempt $nextAttempt")
        rewatchRetryTask = ZookeeperDiscoveryClient.rewatchExecutor.schedule(
          new Runnable {
            override def run(): Unit = runRewatchAttempt(reason, nextAttempt, generation)
          },
          ZookeeperDiscoveryClient.RewatchNodeRetryIntervalMs,
          TimeUnit.MILLISECONDS)
      } else {
        warn(s"Failed to rewatch service node for $reason after $attempt attempt(s)")
        rewatchRetryTask = null
      }
    } catch {
      case NonFatal(e) =>
        warn(s"Failed to set DeRegisterWatcher for $reason at attempt $attempt", e)
        rewatchRetryTask = null
    }
  }

  // A rewatch attempt is still active only if no newer RECONNECTED superseded it and the service
  // node has not been torn down. Reads the volatile fields here (on the rewatch executor thread),
  // never from an anonymous inner class.
  private def rewatchStillActive(generation: Long): Boolean = {
    generation == rewatchGeneration.get() && serviceNode != null && watcher != null
  }

  private def cancelRewatchRetry(): Unit = {
    rewatchGeneration.incrementAndGet()
    val task = rewatchRetryTask
    rewatchRetryTask = null
    if (task != null) {
      task.cancel(false)
    }
  }

  @VisibleForTesting
  private[kyuubi] def rewatchServiceNodeOnce(reason: String): Boolean = {
    try {
      watchServiceNode(reason)
    } catch {
      case NonFatal(e) =>
        warn(s"Failed to set DeRegisterWatcher for $reason", e)
        false
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
      } else if (event.getType == Watcher.Event.EventType.NodeCreated) {
        // When the service node was absent at rewatch time, exists(path, watcher) leaves a
        // one-shot watch that PersistentNode's recovery createNode consumes via NodeCreated.
        // The DeRegisterWatcher must rearm itself here, otherwise the next NodeDeleted would be
        // missed until the following scheduled retry. Reuse the same non-throwing helper.
        info(s"This Kyuubi instance $instance sees its service node recreated;" +
          " rearming DeRegisterWatcher")
        ZookeeperDiscoveryClient.this.rewatchServiceNodeOnce("service node recreated")
      } else if (event.getType == Watcher.Event.EventType.NodeDataChanged) {
        warn(s"This Kyuubi instance $instance now receives the NodeDataChanged event")
        ZookeeperDiscoveryClient.this.watchNode()
      }
    }
  }
}

object ZookeeperDiscoveryClient extends Logging {
  final private val RewatchNodeMaxRetries = 5
  final private val RewatchNodeRetryIntervalMs = 1000L
  // Dedicated executor for RECONNECTED rewatch work, intentionally separate from the shared
  // connectionChecker: a LOST graceful-stop task may block that single thread waiting for open
  // sessions to drain, which would otherwise starve rewatch retries of binary/HTTP discovery
  // clients sharing the static executor.
  final private lazy val rewatchExecutor =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor("zk-rewatch")
  final private lazy val connectionChecker =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor("zk-connection-checker")
}
