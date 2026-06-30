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

package org.apache.kyuubi.server

import scala.util.Properties

import com.codahale.metrics.{Metric, MetricRegistry}
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.security.UserGroupInformation

import org.apache.kyuubi._
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf.{FRONTEND_PROTOCOLS, FrontendProtocols, KYUUBI_KUBERNETES_CONF_PREFIX}
import org.apache.kyuubi.config.KyuubiConf.FrontendProtocols._
import org.apache.kyuubi.events.{EventBus, KyuubiServerInfoEvent, ServerEventHandlerRegister}
import org.apache.kyuubi.ha.HighAvailabilityConf._
import org.apache.kyuubi.ha.client.{AuthTypes, ServiceDiscovery}
import org.apache.kyuubi.metrics.{MetricsConf, MetricsSystem}
import org.apache.kyuubi.server.metadata.jdbc.JDBCMetadataStoreConf
import org.apache.kyuubi.service.{AbstractBackendService, AbstractFrontendService, Serverable, ServiceState}
import org.apache.kyuubi.service.authentication.{AuthenticationProviderFactory, LdapAuthMetricsRecorder}
import org.apache.kyuubi.session.KyuubiSessionManager
import org.apache.kyuubi.util.{KyuubiHadoopUtils, SignalRegister}
import org.apache.kyuubi.zookeeper.EmbeddedZookeeper

object KyuubiServer extends Logging {
  private[kyuubi] var kyuubiServer: KyuubiServer = _
  @volatile private[kyuubi] var hadoopConf: Configuration = _
  private var commandArgs: Array[String] = Array.empty[String]

  def startServer(conf: KyuubiConf): KyuubiServer = {
    hadoopConf = KyuubiHadoopUtils.newHadoopConf(conf)
    var embeddedZkServer: Option[EmbeddedZookeeper] = None
    if (!ServiceDiscovery.supportServiceDiscovery(conf)) {
      embeddedZkServer = Some(new EmbeddedZookeeper())
      embeddedZkServer.foreach(zkServer => {
        zkServer.initialize(conf)
        zkServer.start()
        conf.set(HA_ADDRESSES, zkServer.getConnectString)
        conf.set(HA_ZK_AUTH_TYPE, AuthTypes.NONE.toString)
      })
    }

    val server = conf.get(KyuubiConf.SERVER_NAME) match {
      case Some(s) => new KyuubiServer(s)
      case _ => new KyuubiServer()
    }
    try {
      server.initialize(conf)
    } catch {
      case e: Exception =>
        embeddedZkServer.filter(_.getServiceState == ServiceState.STARTED).foreach(_.stop())
        throw e
    }
    server.start()
    Utils.addShutdownHook(() => server.stop(), Utils.SERVER_SHUTDOWN_PRIORITY)
    server
  }

  def main(args: Array[String]): Unit = {
    info(
      """
        |                  Welcome to
        |  __  __                           __
        | /\ \/\ \                         /\ \      __
        | \ \ \/'/'  __  __  __  __  __  __\ \ \____/\_\
        |  \ \ , <  /\ \/\ \/\ \/\ \/\ \/\ \\ \ '__`\/\ \
        |   \ \ \\`\\ \ \_\ \ \ \_\ \ \ \_\ \\ \ \L\ \ \ \
        |    \ \_\ \_\/`____ \ \____/\ \____/ \ \_,__/\ \_\
        |     \/_/\/_/`/___/> \/___/  \/___/   \/___/  \/_/
        |                /\___/
        |                \/__/
       """.stripMargin)
    info(s"Version: $KYUUBI_VERSION, Revision: $REVISION ($REVISION_TIME), Branch: $BRANCH," +
      s" Java: $JAVA_COMPILE_VERSION, Scala: $SCALA_COMPILE_VERSION," +
      s" Spark: $SPARK_COMPILE_VERSION, Hadoop: $HADOOP_COMPILE_VERSION," +
      s" Hive: $HIVE_COMPILE_VERSION, Flink: $FLINK_COMPILE_VERSION," +
      s" Trino: $TRINO_COMPILE_VERSION")
    info(s"Using Scala ${Properties.versionString}, ${Properties.javaVmName}," +
      s" ${Properties.javaVersion}")
    SignalRegister.registerLogger(logger)

    commandArgs = args

    // register conf entries
    JDBCMetadataStoreConf
    val conf = createKyuubiConf()
    UserGroupInformation.setConfiguration(KyuubiHadoopUtils.newHadoopConf(conf))
    startServer(conf)
  }

  private[kyuubi] def getHadoopConf(): Configuration = {
    hadoopConf
  }

  private[kyuubi] def reloadHadoopConf(): Unit = synchronized {
    val _hadoopConf = KyuubiHadoopUtils.newHadoopConf(createKyuubiConf())
    hadoopConf = _hadoopConf
  }

  private[kyuubi] def refreshUserDefaultsConf(): Unit = kyuubiServer.conf.synchronized {
    val existedUserDefaults = kyuubiServer.conf.getAllUserDefaults
    val refreshedUserDefaults = createKyuubiConf().getAllUserDefaults
    refreshConfig("user defaults", existedUserDefaults, refreshedUserDefaults)
  }

  private[kyuubi] def refreshKubernetesConf(): Unit = kyuubiServer.conf.synchronized {
    val existedKubernetesConf =
      kyuubiServer.conf.getAll.filter(_._1.startsWith(KYUUBI_KUBERNETES_CONF_PREFIX))
    val refreshedKubernetesConf =
      createKyuubiConf().getAll.filter(_._1.startsWith(KYUUBI_KUBERNETES_CONF_PREFIX))
    refreshConfig("kubernetes", existedKubernetesConf, refreshedKubernetesConf)
    kyuubiServer.backendService.sessionManager.asInstanceOf[KyuubiSessionManager].applicationManager
      .getKubernetesApplicationOperation.foreach(_.initializeKubernetesClient(kyuubiServer.conf))
  }

  private def refreshConfig(
      configDomain: String,
      existing: Map[String, String],
      refreshed: Map[String, String]): Unit = {
    var (unsetCount, updatedCount, addedCount) = (0, 0, 0)
    for ((k, _) <- existing if !refreshed.contains(k)) {
      kyuubiServer.conf.unset(k)
      unsetCount = unsetCount + 1
    }
    for ((k, v) <- refreshed) {
      if (existing.contains(k)) {
        if (!StringUtils.equals(existing.get(k).orNull, v)) {
          updatedCount = updatedCount + 1
        }
      } else {
        addedCount = addedCount + 1
      }
      kyuubiServer.conf.set(k, v)
    }
    info(s"Refreshed $configDomain configs with changes of " +
      s"unset: $unsetCount, updated: $updatedCount, added: $addedCount")
  }

  private[kyuubi] def refreshUnlimitedUsers(): Unit = synchronized {
    val sessionMgr = kyuubiServer.backendService.sessionManager.asInstanceOf[KyuubiSessionManager]
    val existingUnlimitedUsers = sessionMgr.getUnlimitedUsers
    sessionMgr.refreshUnlimitedUsers(createKyuubiConf())
    val refreshedUnlimitedUsers = sessionMgr.getUnlimitedUsers
    info(s"Refreshed unlimited users from $existingUnlimitedUsers to $refreshedUnlimitedUsers")
  }

  private[kyuubi] def refreshDenyUsers(): Unit = synchronized {
    val sessionMgr = kyuubiServer.backendService.sessionManager.asInstanceOf[KyuubiSessionManager]
    val existingDenyUsers = sessionMgr.getDenyUsers
    sessionMgr.refreshDenyUsers(createKyuubiConf())
    val refreshedDenyUsers = sessionMgr.getDenyUsers
    info(s"Refreshed deny users from $existingDenyUsers to $refreshedDenyUsers")
  }

  private[kyuubi] def refreshDenyIps(): Unit = synchronized {
    val sessionMgr = kyuubiServer.backendService.sessionManager.asInstanceOf[KyuubiSessionManager]
    val existingDenyIps = sessionMgr.getDenyIps
    sessionMgr.refreshDenyIps(createKyuubiConf())
    val refreshedDenyIps = sessionMgr.getDenyIps
    info(s"Refreshed deny client ips from $existingDenyIps to $refreshedDenyIps")
  }

  private def createKyuubiConf(): KyuubiConf = {
    KyuubiConf().loadFileDefaults().loadFromArgs(commandArgs)
  }
}

class KyuubiServer(name: String) extends Serverable(name) {

  def this() = this(classOf[KyuubiServer].getSimpleName)

  override val backendService: AbstractBackendService =
    new KyuubiBackendService() with BackendServiceMetric

  override lazy val frontendServices: Seq[AbstractFrontendService] =
    conf.get(FRONTEND_PROTOCOLS).map(FrontendProtocols.withName).map {
      case THRIFT_BINARY => new KyuubiTBinaryFrontendService(this)
      case THRIFT_HTTP => new KyuubiTHttpFrontendService(this)
      case REST =>
        warn("REST frontend protocol is experimental, API may change in the future.")
        new KyuubiRestFrontendService(this)
      case TRINO =>
        warn("Trino frontend protocol is experimental.")
        new KyuubiTrinoFrontendService(this)
      case other =>
        throw new UnsupportedOperationException(s"Frontend protocol $other is not supported yet.")
    }

  override def initialize(conf: KyuubiConf): Unit = synchronized {
    val kinit = new KinitAuxiliaryService()
    addService(kinit)

    val periodicGCService = new PeriodicGCService
    addService(periodicGCService)

    if (conf.get(MetricsConf.METRICS_ENABLED)) {
      addService(new MetricsSystem)
    }

    super.initialize(conf)

    initLoggerEventHandler(conf)
  }

  override def start(): Unit = {
    super.start()
    // MetricsSystem is a child service started by super.start(); registering after that
    // call guarantees the metrics are picked up. The accessors return 0 until the LDAP pool
    // is lazily created on the first authentication request, so registration is order-safe.
    installLdapAuthMetricsRecorder()
    registerLdapPoolMetrics()
    KyuubiServer.kyuubiServer = this
    KyuubiServerInfoEvent(this, ServiceState.STARTED).foreach(EventBus.post)
  }

  /**
   * Plugs the auth-event hook in `kyuubi-common` into `MetricsSystem.incCount`, which uses
   * Dropwizard's `Counter` underneath. `DropwizardExports` emits these as
   * `# TYPE name counter` in Prometheus, so `rate()` / `increase()` queries handle process
   * restarts correctly. Dimensions (auth method, failure reason) are dot-suffixes on the
   * base name -- matches the convention of `CONN_OPEN`, `OPERATION_STATE`, etc.
   *
   * The `MetricsSystem` reference is captured once via `tracing` at install time rather
   * than dispatched per call: auth is a hot path under load, and per-event `Option`-check
   * lookups are wasteful when the reference is stable for the JVM lifetime. If metrics is
   * not running at install time (i.e. `MetricsSystem.tracing` is a no-op), the recorder
   * remains at its default `NoOp`; the alternative -- installing a closure that captures a
   * stale reference -- would silently write to a dead registry after a restart.
   *
   * Shutdown ordering invariant (do not break): in [[stop()]] / [[stopServer()]] the
   * teardown order is `super.stop()` then `stopServer()`. `super.stop()` is
   * [[org.apache.kyuubi.service.CompositeService.stop]], which tears down child services
   * in reverse start order. Because [[initialize]] adds [[MetricsSystem]] before
   * `super.initialize(conf)` (which adds the backend and frontend services), the
   * frontends stop first and MetricsSystem stops afterwards within `super.stop()`. By the
   * time MetricsSystem stops, no auth request can arrive at the recorder. Only after
   * `super.stop()` returns does `stopServer()` call [[AuthenticationProviderFactory.close]],
   * which resets the recorder to NoOp. The closure's captured `ms` reference is therefore
   * never invoked after MetricsSystem has stopped.
   *
   * Moving `AuthenticationProviderFactory.close` to run before `super.stop()` would close
   * the LDAP connection pool while the frontends are still accepting requests, causing
   * in-flight auths to fail with pool-closed errors -- a real regression. Do NOT change
   * the order without also addressing that.
   */
  private def installLdapAuthMetricsRecorder(): Unit = MetricsSystem.tracing { ms =>
    import org.apache.kyuubi.metrics.MetricsConstants._
    val ldap = "ldap"
    val successName = MetricRegistry.name(AUTHENTICATION_SUCCESS, ldap)
    val cacheHitName = MetricRegistry.name(AUTHENTICATION_CACHE_HIT, ldap)
    val cacheMissName = MetricRegistry.name(AUTHENTICATION_CACHE_MISS, ldap)
    AuthenticationProviderFactory.setLdapAuthMetricsRecorder(new LdapAuthMetricsRecorder {
      override def recordSuccess(): Unit = ms.incCount(successName)
      override def recordCacheHit(): Unit = ms.incCount(cacheHitName)
      override def recordCacheMiss(): Unit = ms.incCount(cacheMissName)
      override def recordFailure(reason: String): Unit =
        // The `reason` dimension is variable so this single metric name is built per call,
        // but `MetricRegistry.name` is just a string join -- nothing expensive.
        ms.incCount(MetricRegistry.name(AUTHENTICATION_FAILURE, ldap, reason))
    })
  }

  /**
   * LDAP pool metrics. Of the five values [[LDAPConnectionPoolStatistics]] exposes, four
   * are cumulative (checkouts, defunct/expired closures) -- those must be Prometheus
   * counters for `rate()` / `increase()` to be correct across Kyuubi restarts. The fifth
   * (`connections.available`) is a point-in-time queue size and stays a gauge.
   *
   * The SDK owns the underlying values, so we register a [[LdapPoolStatCounter]] whose
   * `getCount()` reads from the SDK statistics on each scrape; `inc`/`dec` are
   * [[UnsupportedOperationException]] so a future caller cannot silently drop an
   * increment by going through [[org.apache.kyuubi.metrics.MetricsSystem.incCount]] on
   * one of the names below (which would otherwise resolve to the existing
   * [[LdapPoolStatCounter]] via `MetricRegistry.counter(name)`).
   *
   * Contract: the four cumulative metric names below are READ-ONLY -- their values flow
   * one-way from `LDAPConnectionPoolStatistics` into the registry on every scrape. Do not
   * call `MetricsSystem.incCount` on any of them; the registered counter will throw.
   */
  private def registerLdapPoolMetrics(): Unit = MetricsSystem.tracing { ms =>
    val prefix = "kyuubi.authentication.ldap.pool"
    val connectionsAvailable = s"$prefix.connections.available"

    // Read-only counter names -- backed by LdapPoolStatCounter, never call .incCount on these.
    val checkoutsSuccessful = s"$prefix.checkouts.successful"
    val checkoutsFailed = s"$prefix.checkouts.failed"
    val connectionsClosedDefunct = s"$prefix.connections.closed.defunct"
    val connectionsClosedExpired = s"$prefix.connections.closed.expired"

    MetricsSystem.getMetricsRegistry.foreach { registry =>
      // start() may run more than once against the same MetricRegistry (embedded use, or a
      // restart after a partial shutdown). Dropwizard's register(...) throws
      // IllegalArgumentException on a duplicate name, so register each metric only when it is
      // absent to keep a re-start idempotent.
      def registerIfAbsent(name: String, metric: => Metric): Unit =
        if (!registry.getNames.contains(name)) registry.register(name, metric)

      if (!registry.getNames.contains(connectionsAvailable)) {
        ms.registerGauge(
          connectionsAvailable,
          AuthenticationProviderFactory.ldapPoolNumAvailableConnections,
          0L)
      }
      registerIfAbsent(
        checkoutsSuccessful,
        new LdapPoolStatCounter(() =>
          AuthenticationProviderFactory.ldapPoolNumSuccessfulCheckouts))
      registerIfAbsent(
        checkoutsFailed,
        new LdapPoolStatCounter(() =>
          AuthenticationProviderFactory.ldapPoolNumFailedCheckouts))
      registerIfAbsent(
        connectionsClosedDefunct,
        new LdapPoolStatCounter(() =>
          AuthenticationProviderFactory.ldapPoolNumConnectionsClosedDefunct))
      registerIfAbsent(
        connectionsClosedExpired,
        new LdapPoolStatCounter(() =>
          AuthenticationProviderFactory.ldapPoolNumConnectionsClosedExpired))
    }
  }

  override def stop(): Unit = {
    KyuubiServerInfoEvent(this, ServiceState.STOPPED).foreach(EventBus.post)
    super.stop()
  }

  private def initLoggerEventHandler(conf: KyuubiConf): Unit = {
    ServerEventHandlerRegister.registerEventLoggers(conf)
  }

  override protected def stopServer(): Unit = {
    // Release the LDAP connection pool (and any cached auth providers) before the JVM exits.
    // CompositeService.stop has already drained the frontend / backend services, so no
    // authentication request can race with this teardown.
    AuthenticationProviderFactory.close()
    EventBus.deregisterAll()
  }
}

/**
 * A Dropwizard [[com.codahale.metrics.Counter]] whose value is read from a supplier rather
 * than maintained internally. Used to register the cumulative counters of
 * [[com.unboundid.ldap.sdk.LDAPConnectionPoolStatistics]] -- which the SDK already owns --
 * as Prometheus type `counter`. The supplier is authoritative; inc/dec are explicitly
 * unsupported so a future call site that tries `MetricsSystem.incCount` against one of the
 * registered names fails loudly instead of silently dropping the increment (the
 * MetricRegistry would otherwise hand back this instance and the no-op .inc() would mask
 * the bug).
 *
 * Lives in this file because `kyuubi-common` does not depend on `metrics-core`; promoting
 * this class to a shared location would require adding that dependency.
 */
final private class LdapPoolStatCounter(read: () => Long) extends com.codahale.metrics.Counter {
  // Empty parens (not nullary) to match Java's `long getCount()`; a nullary override
  // triggers -Xlint:nullary-override, which is fatal under -Xfatal-warnings on Scala 2.13.
  override def getCount(): Long = read()
  override def inc(): Unit = throw new UnsupportedOperationException(
    "LdapPoolStatCounter is read-only; value sourced from LDAPConnectionPoolStatistics")
  override def inc(n: Long): Unit = throw new UnsupportedOperationException(
    "LdapPoolStatCounter is read-only; value sourced from LDAPConnectionPoolStatistics")
  override def dec(): Unit = throw new UnsupportedOperationException(
    "LdapPoolStatCounter is read-only; value sourced from LDAPConnectionPoolStatistics")
  override def dec(n: Long): Unit = throw new UnsupportedOperationException(
    "LdapPoolStatCounter is read-only; value sourced from LDAPConnectionPoolStatistics")
}
