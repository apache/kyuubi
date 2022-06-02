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

package org.apache.kyuubi.ha.client.etcd

import java.util.concurrent.TimeUnit

import scala.collection.JavaConverters._
import scala.concurrent.TimeoutException

import EtcdUtils._
import com.google.common.annotations.VisibleForTesting
import io.etcd.jetcd.Client
import io.etcd.jetcd.KV
import io.etcd.jetcd.Lease
import io.etcd.jetcd.Lock
import io.etcd.jetcd.Watch
import io.etcd.jetcd.lease.LeaseKeepAliveResponse
import io.etcd.jetcd.options.DeleteOption
import io.etcd.jetcd.options.GetOption
import io.etcd.jetcd.options.PutOption
import io.etcd.jetcd.watch.WatchEvent
import io.etcd.jetcd.watch.WatchResponse
import io.grpc.stub.StreamObserver

import org.apache.kyuubi.KYUUBI_VERSION
import org.apache.kyuubi.KyuubiException
import org.apache.kyuubi.KyuubiSQLException
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf.ENGINE_INIT_TIMEOUT
import org.apache.kyuubi.ha.HighAvailabilityConf
import org.apache.kyuubi.ha.HighAvailabilityConf.HA_ENGINE_REF_ID
import org.apache.kyuubi.ha.client.DiscoveryClient
import org.apache.kyuubi.ha.client.DiscoveryPaths
import org.apache.kyuubi.ha.client.ServiceDiscovery
import org.apache.kyuubi.ha.client.ServiceNodeInfo
import org.apache.kyuubi.ha.client.etcd.EtcdDiscoveryClient._

class EtcdDiscoveryClient(conf: KyuubiConf) extends DiscoveryClient {

  case class ServiceNode(path: String, lease: Long)

  var client: Client = _
  var kvClient: KV = _
  var lockClient: Lock = _
  var leaseClient: Lease = _
  var serviceNode: ServiceNode = _

  var leaseTTL: Long = _

  def createClient(): Unit = {
    val endpoints = conf.get(HighAvailabilityConf.HA_ADDRESSES)
    client = Client.builder.endpoints(endpoints).build
    kvClient = client.getKVClient()
    lockClient = client.getLockClient()
    leaseClient = client.getLeaseClient()

    leaseTTL = conf.get(HighAvailabilityConf.HA_ETCD_LEASE_TIMEOUT) / 1000
  }

  def closeClient(): Unit = {
    if (client != null) {
      client.close()
    }
  }

  def create(path: String, mode: String, createParent: Boolean = true): String = {
    // createParent can not effect here
    mode match {
      case "PERSISTENT" => kvClient.put(path, path).get()
      case m => throw new KyuubiException(s"Create mode $m is not support in etcd!")
    }
    path
  }

  def getData(path: String): String = {
    val response = kvClient.get(path).get()
    if (response.getKvs.isEmpty) {
      throw new KyuubiException(s"Key[$path] not exists in ETCD, please check it.")
    } else {
      response.getKvs.get(0).getValue
    }
  }

  def getChildren(path: String): List[String] = {
    val kvs = kvClient.get(
      path,
      GetOption.newBuilder().isPrefix(true).build()).get().getKvs
    if (kvs.isEmpty) {
      List.empty
    } else {
      kvs.asScala.map(kv => byteSequenceToString(kv.getKey).stripPrefix(path).stripPrefix("/"))
        .filter(key => key.nonEmpty && !key.startsWith("lock")).toList
    }
  }

  def pathExists(path: String): Boolean = {
    !pathNonExists(path)
  }

  def pathNonExists(path: String): Boolean = {
    kvClient.get(path).get().getKvs.isEmpty
  }

  def delete(path: String, deleteChildren: Boolean = false): Unit = {
    kvClient.delete(
      path,
      DeleteOption.newBuilder().isPrefix(deleteChildren).build()).get()
  }

  def monitorState(serviceDiscovery: ServiceDiscovery): Unit = {
    // not need with etcd
  }

  def tryWithLock[T](
      lockPath: String,
      timeout: Long)(f: => T): T = {
    // the default unit is millis, covert to seconds.
    // add more 3 second for leaseTime to make client fast fail
    val leaseTime = timeout / 1000 + 3
    // if the lease expires, the lock is automatically released.
    val leaseId = leaseClient.grant(leaseTime).get().getID()
    try {
      try {
        // Acquire a lease. If no leases are available, this method blocks until either the
        // maximum number of leases is increased or another client/process closes a lease

        // will throw TimeoutException when we are get lock timeout
        lockClient.lock(lockPath, leaseId).get(timeout, TimeUnit.MILLISECONDS)
      } catch {
        case _: TimeoutException =>
          throw KyuubiSQLException(s"Timeout to lock on path [$lockPath] after " +
            s"$timeout ms. There would be some problem that other session may " +
            s"create engine timeout.")
        case e: Exception =>
          throw new KyuubiException(s"Lock failed on path [$lockPath]", e)
      }
      f
    } finally {
      try {
        lockClient.unlock(lockPath).get()
        leaseClient.revoke(leaseId).get()
      } catch {
        case e: Exception => throw new KyuubiException(e.getMessage, e.getCause)
      }
    }
  }

  def getServerHost(namespace: String): Option[(String, Int)] = {
    // TODO: use last one because to avoid touching some maybe-crashed engines
    // We need a big improvement here.
    getServiceNodesInfo(namespace, Some(1), silent = true) match {
      case Seq(sn) => Some((sn.host, sn.port))
      case _ => None
    }
  }

  def getEngineByRefId(
      namespace: String,
      engineRefId: String): Option[(String, Int)] = {
    getServiceNodesInfo(namespace, silent = true)
      .find(_.engineRefId.exists(_.equals(engineRefId)))
      .map(data => (data.host, data.port))
  }

  def getServiceNodesInfo(
      namespace: String,
      sizeOpt: Option[Int] = None,
      silent: Boolean = false): Seq[ServiceNodeInfo] = {
    try {
      val hosts = getChildren(DiscoveryPaths.makePath(null, namespace))
      val size = sizeOpt.getOrElse(hosts.size)
      hosts.takeRight(size).map { p =>
        val path = DiscoveryPaths.makePath(namespace, p)
        val instance = getData(path)
        val (host, port) = DiscoveryClient.parseInstanceHostPort(instance)
        val version = p.split(";").find(_.startsWith("version=")).map(_.stripPrefix("version="))
        val engineRefId = p.split(";").find(_.startsWith("refId=")).map(_.stripPrefix("refId="))
        info(s"Get service instance:$instance and version:$version under $namespace")
        ServiceNodeInfo(namespace, p, host, port, version, engineRefId)
      }
    } catch {
      case _: Exception if silent => Nil
      case e: Exception =>
        error(s"Failed to get service node info", e)
        Nil
    }
  }

  def registerService(
      conf: KyuubiConf,
      namespace: String,
      serviceDiscovery: ServiceDiscovery,
      version: Option[String] = None,
      external: Boolean = false): Unit = {
    val instance = serviceDiscovery.fe.connectionUrl
    val watcher = new DeRegisterWatcher(instance, serviceDiscovery)

    val serviceNode = createPersistentNode(conf, namespace, instance, version, external)

    client.getWatchClient.watch(serviceNode.path, watcher)

    if (pathNonExists(serviceNode.path)) {
      // No node exists, throw exception
      throw new KyuubiException(s"Unable to create keyValue for this Kyuubi " +
        s"instance[${instance}] on ETCD.")
    }
  }

  def deregisterService(): Unit = {
    // close the EPHEMERAL_SEQUENTIAL node in etcd
    if (serviceNode != null) {
      if (serviceNode.lease != LEASE_NULL_VALUE) {
        client.getLeaseClient.revoke(serviceNode.lease)
        delete(serviceNode.path)
      }
      serviceNode = null
    }
  }

  def postDeregisterService(namespace: String): Boolean = {
    if (namespace != null) {
      delete(DiscoveryPaths.makePath(null, namespace), true)
      true
    } else {
      false
    }
  }

  def createAndGetServiceNode(
      conf: KyuubiConf,
      namespace: String,
      instance: String,
      version: Option[String] = None,
      external: Boolean = false): String = {
    createPersistentNode(conf, namespace, instance, version, external).path
  }

  @VisibleForTesting
  def startSecretNode(
      createMode: String,
      basePath: String,
      initData: String,
      useProtection: Boolean = false): Unit = {
    client.getKVClient.put(basePath, initData).get()
  }

  private def createPersistentNode(
      conf: KyuubiConf,
      namespace: String,
      instance: String,
      version: Option[String] = None,
      external: Boolean = false): ServiceNode = {
    val ns = DiscoveryPaths.makePath(null, namespace)
    create(ns, "PERSISTENT")

    val session = conf.get(HA_ENGINE_REF_ID)
      .map(refId => s"refId=$refId;").getOrElse("")
    val pathPrefix = DiscoveryPaths.makePath(
      namespace,
      s"serviceUri=$instance;version=${version.getOrElse(KYUUBI_VERSION)};${session}sequence=")
    val znodeData = instance

    var leaseId: Long = LEASE_NULL_VALUE
    var realPath: String = null
    // Use the same of engine init timeout
    val timeout = conf.get(ENGINE_INIT_TIMEOUT)
    // lock to get instance sequence
    tryWithLock(s"$ns$LOCK_PATH_SUFFIX", timeout) {
      val instances = getChildren(pathPrefix).map(_.stripPrefix(pathPrefix).toLong)
      val sequence: Long = if (instances.isEmpty) 0 else instances.max + 1
      realPath = s"$pathPrefix${"%010d".format(sequence)}"

      if (external) {
        client.getKVClient.put(realPath, znodeData).get()
      } else {
        leaseId = client.getLeaseClient.grant(leaseTTL).get().getID
        client.getLeaseClient.keepAlive(
          leaseId,
          new StreamObserver[LeaseKeepAliveResponse] {
            override def onNext(v: LeaseKeepAliveResponse): Unit = Unit // do nothing

            override def onError(throwable: Throwable): Unit = Unit // do nothing

            override def onCompleted(): Unit = Unit // do nothing
          })
        client.getKVClient.put(
          realPath,
          znodeData,
          PutOption.newBuilder().withLeaseId(leaseId).build()).get()
      }
    }
    ServiceNode(realPath, leaseId)
  }

  class DeRegisterWatcher(instance: String, serviceDiscovery: ServiceDiscovery)
    extends Watch.Listener {

    override def onNext(watchResponse: WatchResponse): Unit = {
      watchResponse.getEvents.asScala
        .filter(_.getEventType == WatchEvent.EventType.DELETE).foreach(_ => {
          warn(s"This Kyuubi instance ${instance} is now de-registered from" +
            s" ETCD. The server will be shut down after the last client session completes.")
          serviceDiscovery.stopGracefully()
        })
    }

    override def onError(throwable: Throwable): Unit =
      throw new KyuubiException(throwable.getMessage, throwable.getCause)

    override def onCompleted(): Unit = Unit
  }
}

object EtcdDiscoveryClient {
  final private val LEASE_NULL_VALUE: Long = -1
  final private[etcd] val LOCK_PATH_SUFFIX = "/lock"
}
