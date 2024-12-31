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

package org.apache.kyuubi.config

import java.io.File
import java.time.Duration
import java.util.Locale
import java.util.concurrent.ConcurrentHashMap
import java.util.regex.Pattern

import scala.collection.JavaConverters._
import scala.collection.immutable.TreeMap
import scala.util.matching.Regex

import org.apache.kyuubi.{Logging, Utils}
import org.apache.kyuubi.config.KyuubiConf._
import org.apache.kyuubi.engine.{EngineType, ShareLevel}
import org.apache.kyuubi.engine.deploy.DeployMode
import org.apache.kyuubi.operation.{NoneMode, PlainStyle}
import org.apache.kyuubi.service.authentication.{AuthTypes, SaslQOP}

case class KyuubiConf(loadSysDefault: Boolean = true) extends Logging {

  private val settings = new ConcurrentHashMap[String, String]()
  private lazy val reader: ConfigProvider = new ConfigProvider(settings)
  private def loadFromMap(props: Map[String, String]): Unit = {
    settings.putAll(props.asJava)
  }

  if (loadSysDefault) {
    val fromSysDefaults = Utils.getSystemProperties.filterKeys(_.startsWith("kyuubi.")).toMap
    loadFromMap(fromSysDefaults)
  }

  def loadFileDefaults(): KyuubiConf = {
    val maybeConfigFile = Utils.getDefaultPropertiesFile()
    loadFromMap(Utils.getPropertiesFromFile(maybeConfigFile))
    this
  }

  def loadFromArgs(args: Array[String]): KyuubiConf = {
    Utils.fromCommandLineArgs(args, this)
    this
  }

  def set[T](entry: ConfigEntry[T], value: T): KyuubiConf = {
    require(entry != null, "entry cannot be null")
    require(value != null, s"value cannot be null for key: ${entry.key}")
    require(containsConfigEntry(entry), s"$entry is not registered")
    if (settings.put(entry.key, entry.strConverter(value)) == null) {
      logDeprecationWarning(entry.key)
    }
    this
  }

  def set[T](entry: OptionalConfigEntry[T], value: T): KyuubiConf = {
    require(containsConfigEntry(entry), s"$entry is not registered")
    set(entry.key, entry.strConverter(Option(value)))
    this
  }

  def set(key: String, value: String): KyuubiConf = {
    require(key != null, "key cannot be null")
    require(value != null, s"value cannot be null for key: $key")
    if (settings.put(key, value) == null) {
      logDeprecationWarning(key)
    }
    this
  }

  def setIfMissing[T](entry: ConfigEntry[T], value: T): KyuubiConf = {
    require(containsConfigEntry(entry), s"$entry is not registered")
    if (settings.putIfAbsent(entry.key, entry.strConverter(value)) == null) {
      logDeprecationWarning(entry.key)
    }
    this
  }

  def setIfMissing(key: String, value: String): KyuubiConf = {
    require(key != null)
    require(value != null)
    if (settings.putIfAbsent(key, value) == null) {
      logDeprecationWarning(key)
    }
    this
  }

  def get[T](config: ConfigEntry[T]): T = {
    require(containsConfigEntry(config), s"$config is not registered")
    config.readFrom(reader)
  }

  def getOption(key: String): Option[String] = Option(settings.get(key))

  /** unset a parameter from the configuration */
  def unset(key: String): KyuubiConf = {
    settings.remove(key)
    this
  }

  def unset(entry: ConfigEntry[_]): KyuubiConf = {
    require(containsConfigEntry(entry), s"$entry is not registered")
    unset(entry.key)
  }

  /**
   * Get all parameters as map
   * sorted by key in ascending order
   */
  def getAll: Map[String, String] = {
    TreeMap(settings.asScala.toSeq: _*)
  }

  /** Get all envs as map */
  def getEnvs: Map[String, String] = {
    sys.env ++ getAllWithPrefix(KYUUBI_ENGINE_ENV_PREFIX, "")
  }

  /** Get all batch conf as map */
  def getBatchConf(batchType: String): Map[String, String] = {
    val normalizedBatchType = batchType.toLowerCase(Locale.ROOT) match {
      case "pyspark" => "spark"
      case other => other.toLowerCase(Locale.ROOT)
    }
    getAllWithPrefix(s"$KYUUBI_BATCH_CONF_PREFIX.$normalizedBatchType", "")
  }

  /** Get the kubernetes conf for specified kubernetes context and namespace. */
  def getKubernetesConf(context: Option[String], namespace: Option[String]): KyuubiConf = {
    val conf = this.clone
    context.foreach { c =>
      val contextConf =
        getAllWithPrefix(s"$KYUUBI_KUBERNETES_CONF_PREFIX.$c", "").map { case (suffix, value) =>
          s"$KYUUBI_KUBERNETES_CONF_PREFIX.$suffix" -> value
        }
      val contextNamespaceConf = namespace.map { ns =>
        getAllWithPrefix(s"$KYUUBI_KUBERNETES_CONF_PREFIX.$c.$ns", "").map {
          case (suffix, value) =>
            s"$KYUUBI_KUBERNETES_CONF_PREFIX.$suffix" -> value
        }
      }.getOrElse(Map.empty)

      (contextConf ++ contextNamespaceConf).map { case (key, value) =>
        conf.set(key, value)
      }
      conf.set(KUBERNETES_CONTEXT, c)
      conf
    }
    namespace.foreach(ns => conf.set(KUBERNETES_NAMESPACE, ns))
    conf
  }

  /**
   * Retrieve key-value pairs from [[KyuubiConf]] starting with `dropped.remainder`, and put them to
   * the result map with the `dropped` of key being dropped.
   * @param dropped first part of prefix which will dropped for the new key
   * @param remainder second part of the prefix which will be remained in the key
   */
  def getAllWithPrefix(dropped: String, remainder: String): Map[String, String] = {
    getAll.filter { case (k, _) => k.startsWith(s"$dropped.$remainder") }.map {
      case (k, v) => (k.substring(dropped.length + 1), v)
    }
  }

  /**
   * Retrieve user defaults configs in key-value pairs from [[KyuubiConf]] with key prefix "___"
   */
  def getAllUserDefaults: Map[String, String] = {
    getAll.filter { case (k, _) => k.startsWith(USER_DEFAULTS_CONF_QUOTE) }
  }

  /** Copy this object */
  override def clone: KyuubiConf = {
    val cloned = KyuubiConf(false)
    settings.entrySet().asScala.foreach { e =>
      cloned.set(e.getKey, e.getValue)
    }
    cloned
  }

  def getUserDefaults(user: String): KyuubiConf = {
    val cloned = KyuubiConf(false)

    for (e <- settings.entrySet().asScala if !e.getKey.startsWith(USER_DEFAULTS_CONF_QUOTE)) {
      cloned.set(e.getKey, e.getValue)
    }

    for ((k, v) <-
        getAllWithPrefix(s"$USER_DEFAULTS_CONF_QUOTE${user}$USER_DEFAULTS_CONF_QUOTE", "")) {
      cloned.set(k, v)
    }
    serverOnlyConfEntries.foreach(cloned.unset)
    cloned
  }

  /**
   * Logs a warning message if the given config key is deprecated.
   */
  private def logDeprecationWarning(key: String): Unit = {
    KyuubiConf.deprecatedConfigs.get(key).foreach {
      case DeprecatedConfig(configName, version, comment) =>
        logger.warn(
          s"The Kyuubi config '$configName' has been deprecated in Kyuubi v$version " +
            s"and may be removed in the future. $comment")
    }
  }

  def isRESTEnabled: Boolean = get(FRONTEND_PROTOCOLS).contains(FrontendProtocols.REST.toString)
}

/**
 * Note to developers:
 * You need to rerun the test `org.apache.kyuubi.config.AllKyuubiConfiguration` locally if you
 * add or change a config. That can help to update the conf docs.
 */
object KyuubiConf {

  /** a custom directory that contains the [[KYUUBI_CONF_FILE_NAME]] */
  final val KYUUBI_CONF_DIR = "KYUUBI_CONF_DIR"

  /** the default file that contains kyuubi properties */
  final val KYUUBI_CONF_FILE_NAME = "kyuubi-defaults.conf"
  final val KYUUBI_HOME = "KYUUBI_HOME"
  final val KYUUBI_ENGINE_ENV_PREFIX = "kyuubi.engineEnv"
  final val KYUUBI_ENGINE_YARN_MODE_ENV_PREFIX = "kyuubi.engine.yarn.AMEnv"
  final val KYUUBI_BATCH_CONF_PREFIX = "kyuubi.batchConf"
  final val KYUUBI_KUBERNETES_CONF_PREFIX = "kyuubi.kubernetes"
  final val USER_DEFAULTS_CONF_QUOTE = "___"

  private[this] val kyuubiConfEntriesUpdateLock = new Object

  @volatile
  private[this] var kyuubiConfEntries: java.util.Map[String, ConfigEntry[_]] =
    java.util.Collections.emptyMap()

  private var serverOnlyConfEntries: Set[ConfigEntry[_]] = Set()

  private[config] def register(entry: ConfigEntry[_]): Unit =
    kyuubiConfEntriesUpdateLock.synchronized {
      require(
        !kyuubiConfEntries.containsKey(entry.key),
        s"Duplicate ConfigEntry. ${entry.key} has been registered")
      val updatedMap = new java.util.HashMap[String, ConfigEntry[_]](kyuubiConfEntries)
      updatedMap.put(entry.key, entry)
      kyuubiConfEntries = updatedMap
      if (entry.serverOnly) {
        serverOnlyConfEntries += entry
      }
    }

  // For testing only
  private[config] def unregister(entry: ConfigEntry[_]): Unit =
    kyuubiConfEntriesUpdateLock.synchronized {
      val updatedMap = new java.util.HashMap[String, ConfigEntry[_]](kyuubiConfEntries)
      updatedMap.remove(entry.key)
      kyuubiConfEntries = updatedMap
    }

  private[config] def getConfigEntry(key: String): ConfigEntry[_] = {
    kyuubiConfEntries.get(key)
  }

  private[config] def getConfigEntries(): java.util.Collection[ConfigEntry[_]] = {
    kyuubiConfEntries.values()
  }

  private[config] def containsConfigEntry(entry: ConfigEntry[_]): Boolean = {
    getConfigEntry(entry.key) == entry
  }

  def buildConf(key: String): ConfigBuilder = {
    ConfigBuilder(key).onCreate(register)
  }

  val SERVER_PRINCIPAL: OptionalConfigEntry[String] = buildConf("kyuubi.kinit.principal")
    .doc("Name of the Kerberos principal.")
    .version("1.0.0")
    .serverOnly
    .stringConf
    .createOptional

  val SERVER_KEYTAB: OptionalConfigEntry[String] = buildConf("kyuubi.kinit.keytab")
    .doc("Location of Kyuubi server's keytab.")
    .version("1.0.0")
    .serverOnly
    .stringConf
    .createOptional

  val SERVER_SPNEGO_KEYTAB: OptionalConfigEntry[String] = buildConf("kyuubi.spnego.keytab")
    .doc("Keytab file for SPNego principal")
    .version("1.6.0")
    .serverOnly
    .stringConf
    .createOptional

  val SERVER_SPNEGO_PRINCIPAL: OptionalConfigEntry[String] = buildConf("kyuubi.spnego.principal")
    .doc("SPNego service principal, typical value would look like HTTP/_HOST@EXAMPLE.COM." +
      " SPNego service principal would be used when restful Kerberos security is enabled." +
      " This needs to be set only if SPNEGO is to be used in authentication.")
    .version("1.6.0")
    .serverOnly
    .stringConf
    .createOptional

  val KINIT_INTERVAL: ConfigEntry[Long] = buildConf("kyuubi.kinit.interval")
    .doc("How often will the Kyuubi server run `kinit -kt [keytab] [principal]` to renew the" +
      " local Kerberos credentials cache")
    .version("1.0.0")
    .serverOnly
    .timeConf
    .createWithDefaultString("PT1H")

  val KINIT_MAX_ATTEMPTS: ConfigEntry[Int] = buildConf("kyuubi.kinit.max.attempts")
    .doc("How many times will `kinit` process retry")
    .version("1.0.0")
    .intConf
    .createWithDefault(10)

  val OPERATION_IDLE_TIMEOUT: ConfigEntry[Long] = buildConf("kyuubi.operation.idle.timeout")
    .doc("Operation will be closed when it's not accessed for this duration of time")
    .version("1.0.0")
    .timeConf
    .createWithDefault(Duration.ofHours(3).toMillis)

  val CREDENTIALS_RENEWAL_INTERVAL: ConfigEntry[Long] =
    buildConf("kyuubi.credentials.renewal.interval")
      .doc("How often Kyuubi renews one user's delegation tokens")
      .version("1.4.0")
      .timeConf
      .createWithDefault(Duration.ofHours(1).toMillis)

  val CREDENTIALS_RENEWAL_RETRY_WAIT: ConfigEntry[Long] =
    buildConf("kyuubi.credentials.renewal.retry.wait")
      .doc("How long to wait before retrying to fetch new credentials after a failure.")
      .version("1.4.0")
      .timeConf
      .checkValue(t => t > 0, "must be positive integer")
      .createWithDefault(Duration.ofMinutes(1).toMillis)

  val CREDENTIALS_UPDATE_WAIT_TIMEOUT: ConfigEntry[Long] =
    buildConf("kyuubi.credentials.update.wait.timeout")
      .doc("How long to wait until the credentials are ready.")
      .version("1.5.0")
      .timeConf
      .checkValue(t => t > 0, "must be positive integer")
      .createWithDefault(Duration.ofMinutes(1).toMillis)

  val CREDENTIALS_CHECK_INTERVAL: ConfigEntry[Long] =
    buildConf("kyuubi.credentials.check.interval")
      .doc("The interval to check the expiration of cached <user, CredentialsRef> pairs.")
      .version("1.6.0")
      .timeConf
      .checkValue(_ > Duration.ofSeconds(3).toMillis, "Minimum 3 seconds")
      .createWithDefault(Duration.ofMinutes(5).toMillis)

  val CREDENTIALS_IDLE_TIMEOUT: ConfigEntry[Long] =
    buildConf("kyuubi.credentials.idle.timeout")
      .doc("The inactive users' credentials will be expired after a configured timeout")
      .version("1.6.0")
      .timeConf
      .checkValue(_ >= Duration.ofSeconds(3).toMillis, "Minimum 3 seconds")
      .createWithDefault(Duration.ofHours(6).toMillis)

  val CREDENTIALS_HADOOP_FS_ENABLED: ConfigEntry[Boolean] =
    buildConf("kyuubi.credentials.hadoopfs.enabled")
      .doc("Whether to renew Hadoop filesystem delegation tokens")
      .version("1.4.0")
      .booleanConf
      .createWithDefault(true)

  val CREDENTIALS_HADOOP_FS_URIS: ConfigEntry[Seq[String]] =
    buildConf("kyuubi.credentials.hadoopfs.uris")
      .doc("Extra Hadoop filesystem URIs for which to request delegation tokens. " +
        "The filesystem that hosts fs.defaultFS does not need to be listed here.")
      .version("1.4.0")
      .stringConf
      .toSequence()
      .createWithDefault(Nil)

  val CREDENTIALS_HIVE_ENABLED: ConfigEntry[Boolean] =
    buildConf("kyuubi.credentials.hive.enabled")
      .doc("Whether to renew Hive metastore delegation token")
      .version("1.4.0")
      .booleanConf
      .createWithDefault(true)

  // ///////////////////////////////////////////////////////////////////////////////////////////////
  //                              Frontend Service Configuration                                 //
  // ///////////////////////////////////////////////////////////////////////////////////////////////

  object FrontendProtocols extends Enumeration {
    type FrontendProtocol = Value
    val THRIFT_BINARY, THRIFT_HTTP, REST, MYSQL, TRINO = Value
  }

  val FRONTEND_PROTOCOLS: ConfigEntry[Seq[String]] =
    buildConf("kyuubi.frontend.protocols")
      .doc("A comma-separated list for all frontend protocols " +
        "<ul>" +
        " <li>THRIFT_BINARY - HiveServer2 compatible thrift binary protocol.</li>" +
        " <li>THRIFT_HTTP - HiveServer2 compatible thrift http protocol.</li>" +
        " <li>REST - Kyuubi defined REST API(experimental).</li> " +
        " <li>MYSQL - MySQL compatible text protocol(experimental).</li> " +
        " <li>TRINO - Trino compatible http protocol(experimental).</li> " +
        "</ul>")
      .version("1.4.0")
      .stringConf
      .transformToUpperCase
      .toSequence()
      .checkValues(FrontendProtocols)
      .createWithDefault(Seq(
        FrontendProtocols.THRIFT_BINARY.toString,
        FrontendProtocols.REST.toString))

  val FRONTEND_BIND_HOST: OptionalConfigEntry[String] = buildConf("kyuubi.frontend.bind.host")
    .doc("Hostname or IP of the machine on which to run the frontend services.")
    .version("1.0.0")
    .serverOnly
    .stringConf
    .createOptional

  val FRONTEND_ADVERTISED_HOST: OptionalConfigEntry[String] =
    buildConf("kyuubi.frontend.advertised.host")
      .doc("Hostname or IP of the Kyuubi server's frontend services to publish to " +
        "external systems such as the service discovery ensemble and metadata store. " +
        "Use it when you want to advertise a different hostname or IP than the bind host.")
      .version("1.8.0")
      .serverOnly
      .stringConf
      .createOptional

  val FRONTEND_THRIFT_BINARY_BIND_HOST: ConfigEntry[Option[String]] =
    buildConf("kyuubi.frontend.thrift.binary.bind.host")
      .doc("Hostname or IP of the machine on which to run the thrift frontend service " +
        "via the binary protocol.")
      .version("1.4.0")
      .serverOnly
      .fallbackConf(FRONTEND_BIND_HOST)

  val FRONTEND_THRIFT_BINARY_SSL_ENABLED: ConfigEntry[Boolean] =
    buildConf("kyuubi.frontend.thrift.binary.ssl.enabled")
      .doc("Set this to true for using SSL encryption in thrift binary frontend server.")
      .version("1.7.0")
      .booleanConf
      .createWithDefault(false)

  val FRONTEND_SSL_KEYSTORE_PATH: OptionalConfigEntry[String] =
    buildConf("kyuubi.frontend.ssl.keystore.path")
      .doc("SSL certificate keystore location.")
      .version("1.7.0")
      .stringConf
      .createOptional

  val FRONTEND_SSL_KEYSTORE_PASSWORD: OptionalConfigEntry[String] =
    buildConf("kyuubi.frontend.ssl.keystore.password")
      .doc("SSL certificate keystore password.")
      .version("1.7.0")
      .serverOnly
      .stringConf
      .createOptional

  val FRONTEND_SSL_KEYSTORE_TYPE: OptionalConfigEntry[String] =
    buildConf("kyuubi.frontend.ssl.keystore.type")
      .doc("SSL certificate keystore type.")
      .version("1.7.0")
      .stringConf
      .createOptional

  val FRONTEND_SSL_KEYSTORE_ALGORITHM: OptionalConfigEntry[String] =
    buildConf("kyuubi.frontend.ssl.keystore.algorithm")
      .doc("SSL certificate keystore algorithm.")
      .version("1.7.0")
      .stringConf
      .createOptional

  val FRONTEND_THRIFT_BINARY_SSL_DISALLOWED_PROTOCOLS: ConfigEntry[Set[String]] =
    buildConf("kyuubi.frontend.thrift.binary.ssl.disallowed.protocols")
      .doc("SSL versions to disallow for Kyuubi thrift binary frontend.")
      .version("1.7.0")
      .stringConf
      .toSet()
      .createWithDefault(Set("SSLv2", "SSLv3"))

  val FRONTEND_THRIFT_BINARY_SSL_INCLUDE_CIPHER_SUITES: ConfigEntry[Seq[String]] =
    buildConf("kyuubi.frontend.thrift.binary.ssl.include.ciphersuites")
      .doc("A comma-separated list of include SSL cipher suite names for thrift binary frontend.")
      .version("1.7.0")
      .stringConf
      .toSequence()
      .createWithDefault(Nil)

  @deprecated("using kyuubi.frontend.thrift.binary.bind.port instead", "1.4.0")
  val FRONTEND_BIND_PORT: ConfigEntry[Int] = buildConf("kyuubi.frontend.bind.port")
    .doc("(deprecated) Port of the machine on which to run the thrift frontend service " +
      "via the binary protocol.")
    .version("1.0.0")
    .serverOnly
    .intConf
    .checkValue(p => p == 0 || (p > 1024 && p < 65535), "Invalid Port number")
    .createWithDefault(10009)

  val FRONTEND_THRIFT_BINARY_BIND_PORT: ConfigEntry[Int] =
    buildConf("kyuubi.frontend.thrift.binary.bind.port")
      .doc("Port of the machine on which to run the thrift frontend service " +
        "via the binary protocol.")
      .version("1.4.0")
      .serverOnly
      .fallbackConf(FRONTEND_BIND_PORT)

  val FRONTEND_THRIFT_HTTP_BIND_HOST: ConfigEntry[Option[String]] =
    buildConf("kyuubi.frontend.thrift.http.bind.host")
      .doc("Hostname or IP of the machine on which to run the thrift frontend service " +
        "via http protocol.")
      .version("1.6.0")
      .serverOnly
      .fallbackConf(FRONTEND_BIND_HOST)

  val FRONTEND_THRIFT_HTTP_BIND_PORT: ConfigEntry[Int] =
    buildConf("kyuubi.frontend.thrift.http.bind.port")
      .doc("Port of the machine on which to run the thrift frontend service via http protocol.")
      .version("1.6.0")
      .serverOnly
      .intConf
      .checkValue(p => p == 0 || (p > 1024 && p < 65535), "Invalid Port number")
      .createWithDefault(10010)

  val FRONTEND_MIN_WORKER_THREADS: ConfigEntry[Int] =
    buildConf("kyuubi.frontend.min.worker.threads")
      .doc("(deprecated) Minimum number of threads in the frontend worker thread pool for " +
        "the thrift frontend service")
      .version("1.0.0")
      .intConf
      .createWithDefault(9)

  val FRONTEND_THRIFT_MIN_WORKER_THREADS: ConfigEntry[Int] =
    buildConf("kyuubi.frontend.thrift.min.worker.threads")
      .doc("Minimum number of threads in the frontend worker thread pool for the thrift " +
        "frontend service")
      .version("1.4.0")
      .fallbackConf(FRONTEND_MIN_WORKER_THREADS)

  val FRONTEND_MAX_WORKER_THREADS: ConfigEntry[Int] =
    buildConf("kyuubi.frontend.max.worker.threads")
      .doc("(deprecated) Maximum number of threads in the frontend worker thread pool for " +
        "the thrift frontend service")
      .version("1.0.0")
      .intConf
      .createWithDefault(999)

  val FRONTEND_THRIFT_MAX_WORKER_THREADS: ConfigEntry[Int] =
    buildConf("kyuubi.frontend.thrift.max.worker.threads")
      .doc("Maximum number of threads in the frontend worker thread pool for the thrift " +
        "frontend service")
      .version("1.4.0")
      .fallbackConf(FRONTEND_MAX_WORKER_THREADS)

  val FRONTEND_REST_MAX_WORKER_THREADS: ConfigEntry[Int] =
    buildConf("kyuubi.frontend.rest.max.worker.threads")
      .doc("Maximum number of threads in the frontend worker thread pool for the rest " +
        "frontend service")
      .version("1.6.2")
      .fallbackConf(FRONTEND_MAX_WORKER_THREADS)

  val FRONTEND_REST_PROXY_JETTY_CLIENT_IDLE_TIMEOUT: ConfigEntry[Long] =
    buildConf("kyuubi.frontend.rest.proxy.jetty.client.idleTimeout")
      .doc("The idle timeout in milliseconds for Jetty server " +
        "used by the RESTful frontend service.")
      .version("1.10.0")
      .timeConf
      .createWithDefaultString("PT30S")

  val FRONTEND_REST_PROXY_JETTY_CLIENT_MAX_CONNECTIONS: ConfigEntry[Int] =
    buildConf("kyuubi.frontend.rest.proxy.jetty.client.maxConnections")
      .doc("The max number of connections per destination for Jetty server " +
        "used by the RESTful frontend service.")
      .version("1.10.0")
      .intConf
      .createWithDefault(32768)

  val FRONTEND_REST_PROXY_JETTY_CLIENT_MAX_THREADS: ConfigEntry[Int] =
    buildConf("kyuubi.frontend.rest.proxy.jetty.client.maxThreads")
      .doc("The max number of threads of HttpClient's Executor for Jetty server " +
        "used by the RESTful frontend service.")
      .version("1.10.0")
      .intConf
      .createWithDefault(256)

  val FRONTEND_REST_PROXY_JETTY_CLIENT_REQUEST_BUFFER_SIZE: ConfigEntry[Int] =
    buildConf("kyuubi.frontend.rest.proxy.jetty.client.requestBufferSize")
      .doc("Size of the buffer in bytes used to write requests for Jetty server " +
        "used by the RESTful frontend service.")
      .version("1.10.0")
      .intConf
      .createWithDefault(4096)

  val FRONTEND_REST_PROXY_JETTY_CLIENT_RESPONSE_BUFFER_SIZE: ConfigEntry[Int] =
    buildConf("kyuubi.frontend.rest.proxy.jetty.client.responseBufferSize")
      .doc("Size of the buffer in bytes used to read response for Jetty server " +
        "used by the RESTful frontend service.")
      .version("1.10.0")
      .intConf
      .createWithDefault(4096)

  val FRONTEND_REST_PROXY_JETTY_CLIENT_TIMEOUT: ConfigEntry[Long] =
    buildConf("kyuubi.frontend.rest.proxy.jetty.client.timeout")
      .doc("The total timeout in milliseconds for Jetty server " +
        "used by the RESTful frontend service.")
      .version("1.10.0")
      .timeConf
      .createWithDefaultString("PT60S")

  val FRONTEND_REST_JETTY_STOP_TIMEOUT: ConfigEntry[Long] =
    buildConf("kyuubi.frontend.rest.jetty.stopTimeout")
      .doc("Stop timeout for Jetty server used by the RESTful frontend service.")
      .version("1.8.1")
      .timeConf
      .createWithDefaultString("PT5S")

  val FRONTEND_REST_UI_ENABLED: ConfigEntry[Boolean] =
    buildConf("kyuubi.frontend.rest.ui.enabled")
      .doc("Whether to enable Web UI when RESTful protocol is enabled")
      .version("1.10.0")
      .booleanConf
      .createWithDefault(true)

  val FRONTEND_WORKER_KEEPALIVE_TIME: ConfigEntry[Long] =
    buildConf("kyuubi.frontend.worker.keepalive.time")
      .doc("(deprecated) Keep-alive time (in milliseconds) for an idle worker thread")
      .version("1.0.0")
      .timeConf
      .createWithDefault(Duration.ofSeconds(60).toMillis)

  val FRONTEND_THRIFT_WORKER_KEEPALIVE_TIME: ConfigEntry[Long] =
    buildConf("kyuubi.frontend.thrift.worker.keepalive.time")
      .doc("Keep-alive time (in milliseconds) for an idle worker thread")
      .version("1.4.0")
      .fallbackConf(FRONTEND_WORKER_KEEPALIVE_TIME)

  @deprecated("using kyuubi.frontend.thrift.max.message.size instead", "1.4.0")
  val FRONTEND_MAX_MESSAGE_SIZE: ConfigEntry[Int] =
    buildConf("kyuubi.frontend.max.message.size")
      .doc("(deprecated) Maximum message size in bytes a Kyuubi server will accept.")
      .version("1.0.0")
      .intConf
      .createWithDefault(104857600)

  val FRONTEND_THRIFT_MAX_MESSAGE_SIZE: ConfigEntry[Int] =
    buildConf("kyuubi.frontend.thrift.max.message.size")
      .doc("Maximum message size in bytes a Kyuubi server will accept.")
      .version("1.4.0")
      .fallbackConf(FRONTEND_MAX_MESSAGE_SIZE)

  val FRONTEND_THRIFT_CLIENT_MAX_MESSAGE_SIZE: ConfigEntry[Int] =
    buildConf("kyuubi.frontend.thrift.client.max.message.size")
      .doc("Maximum message size in bytes a thrift client will receive.")
      .version("1.9.3")
      .intConf
      .createWithDefault(1 * 1024 * 1024 * 1024) // follow HIVE-26633 to use 1g as default value

  val FRONTEND_THRIFT_HTTP_REQUEST_HEADER_SIZE: ConfigEntry[Int] =
    buildConf("kyuubi.frontend.thrift.http.request.header.size")
      .doc("Request header size in bytes, when using HTTP transport mode. Jetty defaults used.")
      .version("1.6.0")
      .intConf
      .createWithDefault(6 * 1024)

  val FRONTEND_THRIFT_HTTP_RESPONSE_HEADER_SIZE: ConfigEntry[Int] =
    buildConf("kyuubi.frontend.thrift.http.response.header.size")
      .doc("Response header size in bytes, when using HTTP transport mode. Jetty defaults used.")
      .version("1.6.0")
      .intConf
      .createWithDefault(6 * 1024)

  val FRONTEND_THRIFT_HTTP_MAX_IDLE_TIME: ConfigEntry[Long] =
    buildConf("kyuubi.frontend.thrift.http.max.idle.time")
      .doc("Maximum idle time for a connection on the server when in HTTP mode.")
      .version("1.6.0")
      .timeConf
      .createWithDefault(Duration.ofSeconds(1800).toMillis)

  val FRONTEND_THRIFT_HTTP_PATH: ConfigEntry[String] =
    buildConf("kyuubi.frontend.thrift.http.path")
      .doc("Path component of URL endpoint when in HTTP mode.")
      .version("1.6.0")
      .stringConf
      .createWithDefault("cliservice")

  val FRONTEND_THRIFT_HTTP_COMPRESSION_ENABLED: ConfigEntry[Boolean] =
    buildConf("kyuubi.frontend.thrift.http.compression.enabled")
      .doc("Enable thrift http compression via Jetty compression support")
      .version("1.6.0")
      .booleanConf
      .createWithDefault(true)

  val FRONTEND_JETTY_SEND_VERSION_ENABLED: ConfigEntry[Boolean] =
    buildConf("kyuubi.frontend.jetty.sendVersion.enabled")
      .doc("Whether to send Jetty version in HTTP response.")
      .version("1.9.3")
      .booleanConf
      .createWithDefault(true)

  val FRONTEND_THRIFT_HTTP_COOKIE_AUTH_ENABLED: ConfigEntry[Boolean] =
    buildConf("kyuubi.frontend.thrift.http.cookie.auth.enabled")
      .doc("When true, Kyuubi in HTTP transport mode, " +
        "will use cookie-based authentication mechanism")
      .version("1.6.0")
      .booleanConf
      .createWithDefault(true)

  val FRONTEND_THRIFT_HTTP_COOKIE_MAX_AGE: ConfigEntry[Int] =
    buildConf("kyuubi.frontend.thrift.http.cookie.max.age")
      .doc("Maximum age in seconds for server side cookie used by Kyuubi in HTTP mode.")
      .version("1.6.0")
      .intConf
      .createWithDefault(86400)

  val FRONTEND_THRIFT_HTTP_COOKIE_DOMAIN: OptionalConfigEntry[String] =
    buildConf("kyuubi.frontend.thrift.http.cookie.domain")
      .doc("Domain for the Kyuubi generated cookies")
      .version("1.6.0")
      .stringConf
      .createOptional

  val FRONTEND_THRIFT_HTTP_COOKIE_PATH: OptionalConfigEntry[String] =
    buildConf("kyuubi.frontend.thrift.http.cookie.path")
      .doc("Path for the Kyuubi generated cookies")
      .version("1.6.0")
      .stringConf
      .createOptional

  val FRONTEND_THRIFT_HTTP_COOKIE_IS_HTTPONLY: ConfigEntry[Boolean] =
    buildConf("kyuubi.frontend.thrift.http.cookie.is.httponly")
      .doc("HttpOnly attribute of the Kyuubi generated cookie.")
      .version("1.6.0")
      .booleanConf
      .createWithDefault(true)

  val FRONTEND_THRIFT_HTTP_XSRF_FILTER_ENABLED: ConfigEntry[Boolean] =
    buildConf("kyuubi.frontend.thrift.http.xsrf.filter.enabled")
      .doc("If enabled, Kyuubi will block any requests made to it over HTTP " +
        "if an X-XSRF-HEADER header is not present")
      .version("1.6.0")
      .booleanConf
      .createWithDefault(false)

  val FRONTEND_THRIFT_HTTP_USE_SSL: ConfigEntry[Boolean] =
    buildConf("kyuubi.frontend.thrift.http.use.SSL")
      .doc("Set this to true for using SSL encryption in http mode.")
      .version("1.6.0")
      .booleanConf
      .createWithDefault(false)

  val FRONTEND_THRIFT_HTTP_SSL_KEYSTORE_PATH: OptionalConfigEntry[String] =
    buildConf("kyuubi.frontend.thrift.http.ssl.keystore.path")
      .doc("SSL certificate keystore location.")
      .version("1.6.0")
      .withAlternative("kyuubi.frontend.ssl.keystore.path")
      .stringConf
      .createOptional

  val FRONTEND_THRIFT_HTTP_SSL_KEYSTORE_PASSWORD: OptionalConfigEntry[String] =
    buildConf("kyuubi.frontend.thrift.http.ssl.keystore.password")
      .doc("SSL certificate keystore password.")
      .version("1.6.0")
      .serverOnly
      .withAlternative("kyuubi.frontend.ssl.keystore.password")
      .stringConf
      .createOptional

  val FRONTEND_THRIFT_HTTP_SSL_PROTOCOL_BLACKLIST: ConfigEntry[Seq[String]] =
    buildConf("kyuubi.frontend.thrift.http.ssl.protocol.blacklist")
      .doc("SSL Versions to disable when using HTTP transport mode.")
      .version("1.6.0")
      .stringConf
      .toSequence()
      .createWithDefault(Seq("SSLv2", "SSLv3"))

  val FRONTEND_THRIFT_HTTP_SSL_EXCLUDE_CIPHER_SUITES: ConfigEntry[Seq[String]] =
    buildConf("kyuubi.frontend.thrift.http.ssl.exclude.ciphersuites")
      .doc("A comma-separated list of exclude SSL cipher suite names for thrift http frontend.")
      .version("1.7.0")
      .stringConf
      .toSequence()
      .createWithDefault(Nil)

  val FRONTEND_PROXY_HTTP_CLIENT_IP_HEADER: ConfigEntry[String] =
    buildConf("kyuubi.frontend.proxy.http.client.ip.header")
      .doc("The HTTP header to record the real client IP address. If your server is behind a load" +
        " balancer or other proxy, the server will see this load balancer or proxy IP address as" +
        " the client IP address, to get around this common issue, most load balancers or proxies" +
        " offer the ability to record the real remote IP address in an HTTP header that will be" +
        " added to the request for other devices to use. Note that, because the header value can" +
        " be specified to any IP address, so it will not be used for authentication.")
      .version("1.6.0")
      .stringConf
      .createWithDefault("X-Real-IP")

  val AUTHENTICATION_METHOD: ConfigEntry[Seq[String]] = buildConf("kyuubi.authentication")
    .doc("A comma-separated list of client authentication types." +
      "<ul>" +
      " <li>NOSASL: raw transport.</li>" +
      " <li>NONE: no authentication check.</li>" +
      " <li>KERBEROS: Kerberos/GSSAPI authentication.</li>" +
      " <li>CUSTOM: User-defined authentication.</li>" +
      " <li>JDBC: JDBC query authentication.</li>" +
      " <li>LDAP: Lightweight Directory Access Protocol authentication.</li>" +
      "</ul>" +
      "The following tree describes the catalog of each option." +
      "<ul>" +
      "  <li><code>NOSASL</code></li>" +
      "  <li>SASL" +
      "    <ul>" +
      "      <li>SASL/PLAIN</li>" +
      "        <ul>" +
      "          <li><code>NONE</code></li>" +
      "          <li><code>LDAP</code></li>" +
      "          <li><code>JDBC</code></li>" +
      "          <li><code>CUSTOM</code></li>" +
      "        </ul>" +
      "      <li>SASL/GSSAPI" +
      "        <ul>" +
      "          <li><code>KERBEROS</code></li>" +
      "        </ul>" +
      "      </li>" +
      "    </ul>" +
      "  </li>" +
      "</ul>" +
      " Note that: for SASL authentication, KERBEROS and PLAIN auth types are supported" +
      " at the same time, and only the first specified PLAIN auth type is valid.")
    .version("1.0.0")
    .serverOnly
    .stringConf
    .transformToUpperCase
    .toSequence()
    .checkValues(AuthTypes)
    .createWithDefault(Seq(AuthTypes.NONE.toString))

  val AUTHENTICATION_CUSTOM_CLASS: OptionalConfigEntry[String] =
    buildConf("kyuubi.authentication.custom.class")
      .doc("User-defined authentication implementation of " +
        "org.apache.kyuubi.service.authentication.PasswdAuthenticationProvider")
      .version("1.3.0")
      .serverOnly
      .stringConf
      .createOptional

  val AUTHENTICATION_CUSTOM_BASIC_CLASS: ConfigEntry[Option[String]] =
    buildConf("kyuubi.authentication.custom.basic.class")
      .doc("User-defined authentication implementation of " +
        "org.apache.kyuubi.service.authentication.PasswdAuthenticationProvider " +
        "for http basic authentication.")
      .version("1.10.0")
      .serverOnly
      .fallbackConf(AUTHENTICATION_CUSTOM_CLASS)

  val AUTHENTICATION_CUSTOM_BEARER_CLASS: OptionalConfigEntry[String] =
    buildConf("kyuubi.authentication.custom.bearer.class")
      .doc("User-defined authentication implementation of " +
        "org.apache.kyuubi.service.authentication.TokenAuthenticationProvider " +
        "for http bearer authentication.")
      .version("1.10.0")
      .serverOnly
      .stringConf
      .createOptional

  val AUTHENTICATION_LDAP_URL: OptionalConfigEntry[String] =
    buildConf("kyuubi.authentication.ldap.url")
      .doc("SPACE character separated LDAP connection URL(s).")
      .version("1.0.0")
      .serverOnly
      .stringConf
      .createOptional

  val AUTHENTICATION_LDAP_BASE_DN: OptionalConfigEntry[String] =
    buildConf("kyuubi.authentication.ldap.baseDN")
      .withAlternative("kyuubi.authentication.ldap.base.dn")
      .doc("LDAP base DN.")
      .version("1.7.0")
      .serverOnly
      .stringConf
      .createOptional

  val AUTHENTICATION_LDAP_DOMAIN: OptionalConfigEntry[String] =
    buildConf("kyuubi.authentication.ldap.domain")
      .doc("LDAP domain.")
      .version("1.0.0")
      .serverOnly
      .stringConf
      .createOptional

  val AUTHENTICATION_LDAP_GROUP_DN_PATTERN: OptionalConfigEntry[String] =
    buildConf("kyuubi.authentication.ldap.groupDNPattern")
      .doc("COLON-separated list of patterns to use to find DNs for group entities in " +
        "this directory. Use %s where the actual group name is to be substituted for. " +
        "For example: CN=%s,CN=Groups,DC=subdomain,DC=domain,DC=com.")
      .version("1.7.0")
      .serverOnly
      .stringConf
      .createOptional

  val AUTHENTICATION_LDAP_USER_DN_PATTERN: OptionalConfigEntry[String] =
    buildConf("kyuubi.authentication.ldap.userDNPattern")
      .doc("COLON-separated list of patterns to use to find DNs for users in this directory. " +
        "Use %s where the actual group name is to be substituted for. " +
        "For example: CN=%s,CN=Users,DC=subdomain,DC=domain,DC=com.")
      .version("1.7.0")
      .serverOnly
      .stringConf
      .createOptional

  val AUTHENTICATION_LDAP_GROUP_FILTER: ConfigEntry[Set[String]] =
    buildConf("kyuubi.authentication.ldap.groupFilter")
      .doc("COMMA-separated list of LDAP Group names (short name not full DNs). " +
        "For example: HiveAdmins,HadoopAdmins,Administrators")
      .version("1.7.0")
      .serverOnly
      .stringConf
      .toSet()
      .createWithDefault(Set.empty)

  val AUTHENTICATION_LDAP_USER_FILTER: ConfigEntry[Set[String]] =
    buildConf("kyuubi.authentication.ldap.userFilter")
      .doc("COMMA-separated list of LDAP usernames (just short names, not full DNs). " +
        "For example: hiveuser,impalauser,hiveadmin,hadoopadmin")
      .version("1.7.0")
      .serverOnly
      .stringConf
      .toSet()
      .createWithDefault(Set.empty)

  val AUTHENTICATION_LDAP_GUID_KEY: ConfigEntry[String] =
    buildConf("kyuubi.authentication.ldap.guidKey")
      .doc("LDAP attribute name whose values are unique in this LDAP server. " +
        "For example: uid or CN.")
      .version("1.2.0")
      .serverOnly
      .stringConf
      .createWithDefault("uid")

  val AUTHENTICATION_LDAP_GROUP_MEMBERSHIP_KEY: ConfigEntry[String] =
    buildConf("kyuubi.authentication.ldap.groupMembershipKey")
      .doc("LDAP attribute name on the group object that contains the list of distinguished " +
        "names for the user, group, and contact objects that are members of the group. " +
        "For example: member, uniqueMember or memberUid")
      .version("1.7.0")
      .serverOnly
      .stringConf
      .createWithDefault("member")

  val AUTHENTICATION_LDAP_USER_MEMBERSHIP_KEY: OptionalConfigEntry[String] =
    buildConf("kyuubi.authentication.ldap.userMembershipKey")
      .doc("LDAP attribute name on the user object that contains groups of which the user is " +
        "a direct member, except for the primary group, which is represented by the " +
        "primaryGroupId. For example: memberOf")
      .version("1.7.0")
      .serverOnly
      .stringConf
      .createOptional

  val AUTHENTICATION_LDAP_GROUP_CLASS_KEY: ConfigEntry[String] =
    buildConf("kyuubi.authentication.ldap.groupClassKey")
      .doc("LDAP attribute name on the group entry that is to be used in LDAP group searches. " +
        "For example: group, groupOfNames or groupOfUniqueNames.")
      .version("1.7.0")
      .serverOnly
      .stringConf
      .createWithDefault("groupOfNames")

  val AUTHENTICATION_LDAP_CUSTOM_LDAP_QUERY: OptionalConfigEntry[String] =
    buildConf("kyuubi.authentication.ldap.customLDAPQuery")
      .doc("A full LDAP query that LDAP Atn provider uses to execute against LDAP Server. " +
        "If this query returns a null resultset, the LDAP Provider fails the Authentication " +
        "request, succeeds if the user is part of the resultset." +
        "For example: `(&(objectClass=group)(objectClass=top)(instanceType=4)(cn=Domain*))`, " +
        "`(&(objectClass=person)(|(sAMAccountName=admin)" +
        "(|(memberOf=CN=Domain Admins,CN=Users,DC=domain,DC=com)" +
        "(memberOf=CN=Administrators,CN=Builtin,DC=domain,DC=com))))`")
      .version("1.7.0")
      .serverOnly
      .stringConf
      .createOptional

  val AUTHENTICATION_LDAP_BIND_USER: OptionalConfigEntry[String] =
    buildConf("kyuubi.authentication.ldap.binddn")
      .doc("The user with which to bind to the LDAP server, and search for the full domain name " +
        "of the user being authenticated. This should be the full domain name of the user, and " +
        "should have search access across all users in the LDAP tree. If not specified, then " +
        "the user being authenticated will be used as the bind user. " +
        "For example: CN=bindUser,CN=Users,DC=subdomain,DC=domain,DC=com")
      .version("1.7.0")
      .serverOnly
      .stringConf
      .createOptional

  val AUTHENTICATION_LDAP_BIND_PASSWORD: OptionalConfigEntry[String] =
    buildConf("kyuubi.authentication.ldap.bindpw")
      .doc("The password for the bind user, to be used to search for the full name of the " +
        "user being authenticated. If the username is specified, this parameter must also be " +
        "specified.")
      .version("1.7.0")
      .serverOnly
      .stringConf
      .createOptional

  val AUTHENTICATION_JDBC_DRIVER: OptionalConfigEntry[String] =
    buildConf("kyuubi.authentication.jdbc.driver.class")
      .doc("Driver class name for JDBC Authentication Provider.")
      .version("1.6.0")
      .serverOnly
      .stringConf
      .createOptional

  val AUTHENTICATION_JDBC_URL: OptionalConfigEntry[String] =
    buildConf("kyuubi.authentication.jdbc.url")
      .doc("JDBC URL for JDBC Authentication Provider.")
      .version("1.6.0")
      .serverOnly
      .stringConf
      .createOptional

  val AUTHENTICATION_JDBC_USER: OptionalConfigEntry[String] =
    buildConf("kyuubi.authentication.jdbc.user")
      .doc("Database user for JDBC Authentication Provider.")
      .version("1.6.0")
      .serverOnly
      .stringConf
      .createOptional

  val AUTHENTICATION_JDBC_PASSWORD: OptionalConfigEntry[String] =
    buildConf("kyuubi.authentication.jdbc.password")
      .doc("Database password for JDBC Authentication Provider.")
      .version("1.6.0")
      .serverOnly
      .stringConf
      .createOptional

  val AUTHENTICATION_JDBC_QUERY: OptionalConfigEntry[String] =
    buildConf("kyuubi.authentication.jdbc.query")
      .doc("Query SQL template with placeholders " +
        "for JDBC Authentication Provider to execute. " +
        "Authentication passes if the result set is not empty." +
        "The SQL statement must start with the `SELECT` clause. " +
        "Available placeholders are `${user}` and `${password}`.")
      .version("1.6.0")
      .serverOnly
      .stringConf
      .createOptional

  val DELEGATION_KEY_UPDATE_INTERVAL: ConfigEntry[Long] =
    buildConf("kyuubi.delegation.key.update.interval")
      .doc("unused yet")
      .version("1.0.0")
      .timeConf
      .createWithDefault(Duration.ofDays(1).toMillis)

  val DELEGATION_TOKEN_MAX_LIFETIME: ConfigEntry[Long] =
    buildConf("kyuubi.delegation.token.max.lifetime")
      .doc("unused yet")
      .version("1.0.0")
      .timeConf
      .createWithDefault(Duration.ofDays(7).toMillis)

  val DELEGATION_TOKEN_GC_INTERVAL: ConfigEntry[Long] =
    buildConf("kyuubi.delegation.token.gc.interval")
      .doc("unused yet")
      .version("1.0.0")
      .timeConf
      .createWithDefault(Duration.ofHours(1).toMillis)

  val DELEGATION_TOKEN_RENEW_INTERVAL: ConfigEntry[Long] =
    buildConf("kyuubi.delegation.token.renew.interval")
      .doc("unused yet")
      .version("1.0.0")
      .timeConf
      .createWithDefault(Duration.ofDays(7).toMillis)

  val SASL_QOP: ConfigEntry[String] = buildConf("kyuubi.authentication.sasl.qop")
    .doc("Sasl QOP enable higher levels of protection for Kyuubi communication with clients.<ul>" +
      " <li>auth - authentication only (default)</li>" +
      " <li>auth-int - authentication plus integrity protection</li>" +
      " <li>auth-conf - authentication plus integrity and confidentiality protection. This is" +
      " applicable only if Kyuubi is configured to use Kerberos authentication.</li> </ul>")
    .version("1.0.0")
    .serverOnly
    .stringConf
    .checkValues(SaslQOP)
    .transformToLowerCase
    .createWithDefault(SaslQOP.AUTH.toString)

  val FRONTEND_REST_BIND_HOST: ConfigEntry[Option[String]] =
    buildConf("kyuubi.frontend.rest.bind.host")
      .doc("Hostname or IP of the machine on which to run the REST frontend service.")
      .version("1.4.0")
      .serverOnly
      .fallbackConf(FRONTEND_BIND_HOST)

  val FRONTEND_REST_BIND_PORT: ConfigEntry[Int] = buildConf("kyuubi.frontend.rest.bind.port")
    .doc("Port of the machine on which to run the REST frontend service.")
    .version("1.4.0")
    .serverOnly
    .intConf
    .checkValue(p => p == 0 || (p > 1024 && p < 65535), "Invalid Port number")
    .createWithDefault(10099)

  val FRONTEND_MYSQL_BIND_HOST: ConfigEntry[Option[String]] =
    buildConf("kyuubi.frontend.mysql.bind.host")
      .doc("Hostname or IP of the machine on which to run the MySQL frontend service.")
      .version("1.4.0")
      .serverOnly
      .fallbackConf(FRONTEND_BIND_HOST)

  val FRONTEND_MYSQL_BIND_PORT: ConfigEntry[Int] = buildConf("kyuubi.frontend.mysql.bind.port")
    .doc("Port of the machine on which to run the MySQL frontend service.")
    .version("1.4.0")
    .serverOnly
    .intConf
    .checkValue(p => p == 0 || (p > 1024 && p < 65535), "Invalid Port number")
    .createWithDefault(3309)

  /**
   * Specifies an upper bound on the number of Netty threads that Kyuubi requires by default.
   * In practice, only 2-4 cores should be required to transfer roughly 10 Gb/s, and each core
   * that we use will have an initial overhead of roughly 32 MB of off-heap memory, which comes
   * at a premium.
   *
   * Thus, this value should still retain maximum throughput and reduce wasted off-heap memory
   * allocation.
   */
  val MAX_NETTY_THREADS: Int = 8
  val FRONTEND_MYSQL_NETTY_WORKER_THREADS: OptionalConfigEntry[Int] =
    buildConf("kyuubi.frontend.mysql.netty.worker.threads")
      .doc("Number of thread in the netty worker event loop of MySQL frontend service. " +
        s"Use min(cpu_cores, $MAX_NETTY_THREADS) in default.")
      .version("1.4.0")
      .intConf
      .checkValue(
        n => n > 0 && n <= MAX_NETTY_THREADS,
        s"Invalid thread number, must in (0, $MAX_NETTY_THREADS]")
      .createOptional

  val FRONTEND_MYSQL_MIN_WORKER_THREADS: ConfigEntry[Int] =
    buildConf("kyuubi.frontend.mysql.min.worker.threads")
      .doc("Minimum number of threads in the command execution thread pool for the MySQL " +
        "frontend service")
      .version("1.4.0")
      .fallbackConf(FRONTEND_MIN_WORKER_THREADS)

  val FRONTEND_MYSQL_MAX_WORKER_THREADS: ConfigEntry[Int] =
    buildConf("kyuubi.frontend.mysql.max.worker.threads")
      .doc("Maximum number of threads in the command execution thread pool for the MySQL " +
        "frontend service")
      .version("1.4.0")
      .fallbackConf(FRONTEND_MAX_WORKER_THREADS)

  val FRONTEND_MYSQL_WORKER_KEEPALIVE_TIME: ConfigEntry[Long] =
    buildConf("kyuubi.frontend.mysql.worker.keepalive.time")
      .doc("Time(ms) that an idle async thread of the command execution thread pool will wait" +
        " for a new task to arrive before terminating in MySQL frontend service")
      .version("1.4.0")
      .fallbackConf(FRONTEND_WORKER_KEEPALIVE_TIME)

  val FRONTEND_TRINO_BIND_HOST: ConfigEntry[Option[String]] =
    buildConf("kyuubi.frontend.trino.bind.host")
      .doc("Hostname or IP of the machine on which to run the TRINO frontend service.")
      .version("1.7.0")
      .serverOnly
      .fallbackConf(FRONTEND_BIND_HOST)

  val FRONTEND_TRINO_BIND_PORT: ConfigEntry[Int] = buildConf("kyuubi.frontend.trino.bind.port")
    .doc("Port of the machine on which to run the TRINO frontend service.")
    .version("1.7.0")
    .serverOnly
    .intConf
    .checkValue(p => p == 0 || (p > 1024 && p < 65535), "Invalid Port number")
    .createWithDefault(10999)

  val FRONTEND_TRINO_MAX_WORKER_THREADS: ConfigEntry[Int] =
    buildConf("kyuubi.frontend.trino.max.worker.threads")
      .doc("Maximum number of threads in the frontend worker thread pool for the Trino " +
        "frontend service")
      .version("1.7.0")
      .fallbackConf(FRONTEND_MAX_WORKER_THREADS)

  val FRONTEND_TRINO_JETTY_STOP_TIMEOUT: ConfigEntry[Long] =
    buildConf("kyuubi.frontend.trino.jetty.stopTimeout")
      .doc("Stop timeout for Jetty server used by the Trino frontend service.")
      .version("1.8.1")
      .timeConf
      .createWithDefaultString("PT5S")

  val KUBERNETES_CONTEXT: OptionalConfigEntry[String] =
    buildConf("kyuubi.kubernetes.context")
      .doc("The desired context from your kubernetes config file used to configure the K8s " +
        "client for interacting with the cluster.")
      .version("1.6.0")
      .stringConf
      .createOptional

  val KUBERNETES_CONTEXT_ALLOW_LIST: ConfigEntry[Set[String]] =
    buildConf("kyuubi.kubernetes.context.allow.list")
      .doc("The allowed kubernetes context list, if it is empty," +
        " there is no kubernetes context limitation.")
      .version("1.8.0")
      .stringConf
      .toSet()
      .createWithDefault(Set.empty)

  val KUBERNETES_NAMESPACE: ConfigEntry[String] =
    buildConf("kyuubi.kubernetes.namespace")
      .doc("The namespace that will be used for running the kyuubi pods and find engines.")
      .version("1.7.0")
      .stringConf
      .createWithDefault("default")

  val KUBERNETES_NAMESPACE_ALLOW_LIST: ConfigEntry[Set[String]] =
    buildConf("kyuubi.kubernetes.namespace.allow.list")
      .doc("The allowed kubernetes namespace list, if it is empty," +
        " there is no kubernetes namespace limitation.")
      .version("1.8.0")
      .stringConf
      .toSet()
      .createWithDefault(Set.empty)

  val KUBERNETES_MASTER: OptionalConfigEntry[String] =
    buildConf("kyuubi.kubernetes.master.address")
      .doc("The internal Kubernetes master (API server) address to be used for kyuubi.")
      .version("1.7.0")
      .stringConf
      .createOptional

  val KUBERNETES_AUTHENTICATE_OAUTH_TOKEN_FILE: OptionalConfigEntry[String] =
    buildConf("kyuubi.kubernetes.authenticate.oauthTokenFile")
      .doc("Path to the file containing the OAuth token to use when authenticating against " +
        "the Kubernetes API server. Specify this as a path as opposed to a URI " +
        "(i.e. do not provide a scheme)")
      .version("1.7.0")
      .stringConf
      .createOptional

  val KUBERNETES_AUTHENTICATE_OAUTH_TOKEN: OptionalConfigEntry[String] =
    buildConf("kyuubi.kubernetes.authenticate.oauthToken")
      .doc("The OAuth token to use when authenticating against the Kubernetes API server. " +
        "Note that unlike, the other authentication options, this must be the exact string value" +
        " of the token to use for the authentication.")
      .version("1.7.0")
      .stringConf
      .createOptional

  val KUBERNETES_AUTHENTICATE_CLIENT_KEY_FILE: OptionalConfigEntry[String] =
    buildConf("kyuubi.kubernetes.authenticate.clientKeyFile")
      .doc("Path to the client key file for connecting to the Kubernetes API server " +
        "over TLS from the kyuubi. Specify this as a path as opposed to a URI " +
        "(i.e. do not provide a scheme)")
      .version("1.7.0")
      .stringConf
      .createOptional

  val KUBERNETES_AUTHENTICATE_CLIENT_CERT_FILE: OptionalConfigEntry[String] =
    buildConf("kyuubi.kubernetes.authenticate.clientCertFile")
      .doc("Path to the client cert file for connecting to the Kubernetes API server " +
        "over TLS from the kyuubi. Specify this as a path as opposed to a URI " +
        "(i.e. do not provide a scheme)")
      .version("1.7.0")
      .stringConf
      .createOptional

  val KUBERNETES_AUTHENTICATE_CA_CERT_FILE: OptionalConfigEntry[String] =
    buildConf("kyuubi.kubernetes.authenticate.caCertFile")
      .doc("Path to the CA cert file for connecting to the Kubernetes API server " +
        "over TLS from the kyuubi. Specify this as a path as opposed to a URI " +
        "(i.e. do not provide a scheme)")
      .version("1.7.0")
      .stringConf
      .createOptional

  val KUBERNETES_TRUST_CERTIFICATES: ConfigEntry[Boolean] =
    buildConf("kyuubi.kubernetes.trust.certificates")
      .doc("If set to true then client can submit to kubernetes cluster only with token")
      .version("1.7.0")
      .booleanConf
      .createWithDefault(false)

  val KUBERNETES_TERMINATED_APPLICATION_RETAIN_PERIOD: ConfigEntry[Long] =
    buildConf("kyuubi.kubernetes.terminatedApplicationRetainPeriod")
      .doc("The period for which the Kyuubi server retains application information after " +
        "the application terminates.")
      .version("1.7.1")
      .timeConf
      .checkValue(_ > 0, "must be positive number")
      .createWithDefault(Duration.ofMinutes(5).toMillis)

  val KUBERNETES_SPARK_CLEANUP_TERMINATED_DRIVER_POD_KIND_CHECK_INTERVAL: ConfigEntry[Long] =
    buildConf("kyuubi.kubernetes.spark.cleanupTerminatedDriverPod.checkInterval")
      .doc("Kyuubi server use guava cache as the cleanup trigger with time-based eviction, " +
        "but the eviction would not happened until any get/put operation happened. " +
        "This option schedule a daemon thread evict cache periodically.")
      .version("1.8.1")
      .timeConf
      .createWithDefaultString("PT1M")

  val KUBERNETES_SPARK_CLEANUP_TERMINATED_DRIVER_POD_KIND: ConfigEntry[String] =
    buildConf("kyuubi.kubernetes.spark.cleanupTerminatedDriverPod.kind")
      .doc("Kyuubi server will delete the spark driver pod after " +
        s"the application terminates for ${KUBERNETES_TERMINATED_APPLICATION_RETAIN_PERIOD.key}. " +
        "Available options are NONE, ALL, COMPLETED and " +
        "default value is None which means none of the pod will be deleted")
      .version("1.8.1")
      .stringConf
      .createWithDefault(KubernetesCleanupDriverPodStrategy.NONE.toString)

  val KUBERNETES_SPARK_APP_URL_PATTERN: ConfigEntry[String] =
    buildConf("kyuubi.kubernetes.spark.appUrlPattern")
      .doc("The pattern to generate the spark on kubernetes application UI URL. " +
        "The pattern should contain placeholders for the application variables. " +
        "Available placeholders are `{{SPARK_APP_ID}}`, `{{SPARK_DRIVER_SVC}}`, " +
        "`{{KUBERNETES_NAMESPACE}}`, `{{KUBERNETES_CONTEXT}}` and `{{SPARK_UI_PORT}}`.")
      .version("1.10.0")
      .stringConf
      .createWithDefault(
        "http://{{SPARK_DRIVER_SVC}}.{{KUBERNETES_NAMESPACE}}.svc:{{SPARK_UI_PORT}}")

  val KUBERNETES_SPARK_AUTO_CREATE_FILE_UPLOAD_PATH: ConfigEntry[Boolean] =
    buildConf("kyuubi.kubernetes.spark.autoCreateFileUploadPath.enabled")
      .doc("If enabled, Kyuubi server will try to create the " +
        "`spark.kubernetes.file.upload.path` with permission 777 before submitting " +
        "the Spark application.")
      .version("1.11.0")
      .booleanConf
      .createWithDefault(false)

  object KubernetesCleanupDriverPodStrategy extends Enumeration {
    type KubernetesCleanupDriverPodStrategy = Value
    val NONE, ALL, COMPLETED = Value
  }

  val KUBERNETES_APPLICATION_STATE_CONTAINER: ConfigEntry[String] =
    buildConf("kyuubi.kubernetes.application.state.container")
      .doc("The container name to retrieve the application state from.")
      .version("1.8.1")
      .stringConf
      .createWithDefault("spark-kubernetes-driver")

  val KUBERNETES_APPLICATION_STATE_SOURCE: ConfigEntry[String] =
    buildConf("kyuubi.kubernetes.application.state.source")
      .doc("The source to retrieve the application state from. The valid values are " +
        "pod and container. When the pod is in a terminated state, the container state" +
        " will be ignored, and the application state will be determined based on the pod state." +
        " If the source is container and there is container inside the pod " +
        s"with the name of ${KUBERNETES_APPLICATION_STATE_CONTAINER.key}, the application state " +
        s"will be from the matched container state. " +
        s"Otherwise, the application state will be from the pod state.")
      .version("1.8.1")
      .stringConf
      .checkValues(KubernetesApplicationStateSource)
      .createWithDefault(KubernetesApplicationStateSource.POD.toString)

  object KubernetesApplicationStateSource extends Enumeration {
    type KubernetesApplicationStateSource = Value
    val POD, CONTAINER = Value
  }

  // ///////////////////////////////////////////////////////////////////////////////////////////////
  //                                 SQL Engine Configuration                                    //
  // ///////////////////////////////////////////////////////////////////////////////////////////////

  val ENGINE_ERROR_MAX_SIZE: ConfigEntry[Int] =
    buildConf("kyuubi.session.engine.startup.error.max.size")
      .doc("During engine bootstrapping, if an error occurs, using this config to limit" +
        " the length of error message(characters).")
      .version("1.1.0")
      .intConf
      .checkValue(v => v >= 200 && v <= 8192, s"must in [200, 8192]")
      .createWithDefault(8192)

  val ENGINE_LOG_TIMEOUT: ConfigEntry[Long] = buildConf("kyuubi.session.engine.log.timeout")
    .doc("If we use Spark as the engine then the session submit log is the console output of " +
      "spark-submit. We will retain the session submit log until over the config value.")
    .version("1.1.0")
    .timeConf
    .checkValue(_ > 0, "must be positive number")
    .createWithDefault(Duration.ofDays(1).toMillis)

  val ENGINE_SPARK_MAIN_RESOURCE: OptionalConfigEntry[String] =
    buildConf("kyuubi.session.engine.spark.main.resource")
      .doc("The package used to create Spark SQL engine remote application. If it is undefined," +
        " Kyuubi will use the default")
      .version("1.0.0")
      .stringConf
      .createOptional

  val SESSION_CONF_PROFILE: OptionalConfigEntry[Seq[String]] =
    buildConf("kyuubi.session.conf.profile")
      .doc("Specify a profile to load session-level configurations from " +
        "multiple `$KYUUBI_CONF_DIR/kyuubi-session-<profile>.conf` files. " +
        "This configuration will be ignored if the file does not exist. " +
        "This configuration only takes effect when `kyuubi.session.conf.advisor` " +
        "is set as `org.apache.kyuubi.session.FileSessionConfAdvisor`.")
      .version("1.7.0")
      .stringConf
      .toSequence()
      .createOptional

  val SESSION_CONF_FILE_RELOAD_INTERVAL: ConfigEntry[Long] =
    buildConf("kyuubi.session.conf.file.reload.interval")
      .doc("When `FileSessionConfAdvisor` is used, this configuration defines " +
        "the expired time of `$KYUUBI_CONF_DIR/kyuubi-session-<profile>.conf` " +
        "in the cache. After exceeding this value, the file will be reloaded.")
      .version("1.7.0")
      .timeConf
      .createWithDefault(Duration.ofMinutes(10).toMillis)

  val ENGINE_SPARK_MAX_LIFETIME: ConfigEntry[Long] =
    buildConf("kyuubi.session.engine.spark.max.lifetime")
      .doc("Max lifetime for Spark engine, the engine will self-terminate when it reaches the" +
        " end of life. 0 or negative means not to self-terminate.")
      .version("1.6.0")
      .timeConf
      .createWithDefault(0)

  val ENGINE_SPARK_MAX_LIFETIME_GRACEFUL_PERIOD: ConfigEntry[Long] =
    buildConf("kyuubi.session.engine.spark.max.lifetime.gracefulPeriod")
      .doc("Graceful period for Spark engine to wait the connections disconnected after reaching" +
        " the end of life. After the graceful period, all the connections without running" +
        " operations will be forcibly disconnected. 0 or negative means always waiting the" +
        " connections disconnected.")
      .version("1.8.1")
      .timeConf
      .createWithDefault(0)

  val ENGINE_SPARK_MAX_INITIAL_WAIT: ConfigEntry[Long] =
    buildConf("kyuubi.session.engine.spark.max.initial.wait")
      .doc("Max wait time for the initial connection to Spark engine. The engine will" +
        " self-terminate no new incoming connection is established within this time." +
        " This setting only applies at the CONNECTION share level." +
        " 0 or negative means not to self-terminate.")
      .version("1.8.0")
      .timeConf
      .createWithDefault(Duration.ofSeconds(60).toMillis)

  val ENGINE_FLINK_MAIN_RESOURCE: OptionalConfigEntry[String] =
    buildConf("kyuubi.session.engine.flink.main.resource")
      .doc("The package used to create Flink SQL engine remote job. If it is undefined," +
        " Kyuubi will use the default")
      .version("1.4.0")
      .stringConf
      .createOptional

  val ENGINE_FLINK_MAX_ROWS: ConfigEntry[Int] =
    buildConf("kyuubi.session.engine.flink.max.rows")
      .doc("Max rows of Flink query results. For batch queries, rows exceeding the limit " +
        "would be ignored. For streaming queries, the query would be canceled if the limit " +
        "is reached.")
      .version("1.5.0")
      .intConf
      .createWithDefault(1000000)

  val ENGINE_FLINK_FETCH_TIMEOUT: OptionalConfigEntry[Long] =
    buildConf("kyuubi.session.engine.flink.fetch.timeout")
      .doc("Result fetch timeout for Flink engine. If the timeout is reached, the result " +
        "fetch would be stopped and the current fetched would be returned. If no data are " +
        "fetched, a TimeoutException would be thrown.")
      .version("1.8.0")
      .timeConf
      .createOptional

  val ENGINE_TRINO_MAIN_RESOURCE: OptionalConfigEntry[String] =
    buildConf("kyuubi.session.engine.trino.main.resource")
      .doc("The package used to create Trino engine remote job. If it is undefined," +
        " Kyuubi will use the default")
      .version("1.5.0")
      .stringConf
      .createOptional

  val ENGINE_TRINO_CONNECTION_URL: OptionalConfigEntry[String] =
    buildConf("kyuubi.session.engine.trino.connection.url")
      .doc("The server url that Trino engine will connect to")
      .version("1.5.0")
      .stringConf
      .createOptional

  val ENGINE_TRINO_CONNECTION_CATALOG: OptionalConfigEntry[String] =
    buildConf("kyuubi.session.engine.trino.connection.catalog")
      .doc("The default catalog that Trino engine will connect to")
      .version("1.5.0")
      .stringConf
      .createOptional

  val ENGINE_TRINO_CONNECTION_USER: OptionalConfigEntry[String] =
    buildConf("kyuubi.engine.trino.connection.user")
      .doc("The user used for connecting to trino cluster")
      .version("1.9.0")
      .stringConf
      .createOptional

  val ENGINE_TRINO_CONNECTION_PASSWORD: OptionalConfigEntry[String] =
    buildConf("kyuubi.engine.trino.connection.password")
      .doc("The password used for connecting to trino cluster")
      .version("1.8.0")
      .stringConf
      .createOptional

  val ENGINE_TRINO_CONNECTION_KEYSTORE_PATH: OptionalConfigEntry[String] =
    buildConf("kyuubi.engine.trino.connection.keystore.path")
      .doc("The keystore path used for connecting to trino cluster")
      .version("1.8.0")
      .stringConf
      .createOptional

  val ENGINE_TRINO_CONNECTION_KEYSTORE_PASSWORD: OptionalConfigEntry[String] =
    buildConf("kyuubi.engine.trino.connection.keystore.password")
      .doc("The keystore password used for connecting to trino cluster")
      .version("1.8.0")
      .stringConf
      .createOptional

  val ENGINE_TRINO_CONNECTION_KEYSTORE_TYPE: OptionalConfigEntry[String] =
    buildConf("kyuubi.engine.trino.connection.keystore.type")
      .doc("The keystore type used for connecting to trino cluster")
      .version("1.8.0")
      .stringConf
      .createOptional

  val ENGINE_TRINO_CONNECTION_TRUSTSTORE_PATH: OptionalConfigEntry[String] =
    buildConf("kyuubi.engine.trino.connection.truststore.path")
      .doc("The truststore path used for connecting to trino cluster")
      .version("1.8.0")
      .stringConf
      .createOptional

  val ENGINE_TRINO_CONNECTION_TRUSTSTORE_PASSWORD: OptionalConfigEntry[String] =
    buildConf("kyuubi.engine.trino.connection.truststore.password")
      .doc("The truststore password used for connecting to trino cluster")
      .version("1.8.0")
      .stringConf
      .createOptional

  val ENGINE_TRINO_CONNECTION_TRUSTSTORE_TYPE: OptionalConfigEntry[String] =
    buildConf("kyuubi.engine.trino.connection.truststore.type")
      .doc("The truststore type used for connecting to trino cluster")
      .version("1.8.0")
      .stringConf
      .createOptional

  val ENGINE_TRINO_CONNECTION_INSECURE_ENABLED: ConfigEntry[Boolean] =
    buildConf("kyuubi.engine.trino.connection.insecure.enabled")
      .doc("Skip certificate validation when connecting with TLS/HTTPS enabled trino cluster")
      .version("1.9.2")
      .booleanConf
      .createWithDefault(false)

  val ENGINE_TRINO_SHOW_PROGRESS: ConfigEntry[Boolean] =
    buildConf("kyuubi.session.engine.trino.showProgress")
      .doc("When true, show the progress bar and final info in the Trino engine log.")
      .version("1.6.0")
      .booleanConf
      .createWithDefault(true)

  val ENGINE_TRINO_SHOW_PROGRESS_DEBUG: ConfigEntry[Boolean] =
    buildConf("kyuubi.session.engine.trino.showProgress.debug")
      .doc("When true, show the progress debug info in the Trino engine log.")
      .version("1.6.0")
      .booleanConf
      .createWithDefault(false)

  val ENGINE_TRINO_SHOW_PROGRESS_UPDATE_INTERVAL: ConfigEntry[Long] =
    buildConf("kyuubi.session.engine.trino.progress.update.interval")
      .doc("Update period of progress bar.")
      .version("1.10.0")
      .timeConf
      .checkValue(_ >= 200, "Minimum 200 milliseconds")
      .createWithDefault(1000)

  val ENGINE_HIVE_MAIN_RESOURCE: OptionalConfigEntry[String] =
    buildConf("kyuubi.session.engine.hive.main.resource")
      .doc("The package used to create Hive engine remote job. If it is undefined," +
        " Kyuubi will use the default")
      .version("1.6.0")
      .stringConf
      .createOptional

  val ENGINE_LOGIN_TIMEOUT: ConfigEntry[Long] = buildConf("kyuubi.session.engine.login.timeout")
    .doc("The timeout of creating the connection to remote sql query engine")
    .version("1.0.0")
    .timeConf
    .createWithDefault(Duration.ofSeconds(15).toMillis)

  val ENGINE_ALIVE_MAX_FAILURES: ConfigEntry[Int] =
    buildConf("kyuubi.session.engine.alive.max.failures")
      .doc("The maximum number of failures allowed for the engine.")
      .version("1.8.1")
      .intConf
      .checkValue(_ > 0, "Must be positive")
      .createWithDefault(3)

  val ENGINE_ALIVE_PROBE_ENABLED: ConfigEntry[Boolean] =
    buildConf("kyuubi.session.engine.alive.probe.enabled")
      .doc("Whether to enable the engine alive probe, it true, we will create a companion thrift" +
        " client that keeps sending simple requests to check whether the engine is alive.")
      .version("1.6.0")
      .booleanConf
      .createWithDefault(false)

  val ENGINE_ALIVE_PROBE_INTERVAL: ConfigEntry[Long] =
    buildConf("kyuubi.session.engine.alive.probe.interval")
      .doc("The interval for engine alive probe.")
      .version("1.6.0")
      .timeConf
      .createWithDefault(Duration.ofSeconds(10).toMillis)

  val ENGINE_ALIVE_TIMEOUT: ConfigEntry[Long] =
    buildConf("kyuubi.session.engine.alive.timeout")
      .doc("The timeout for engine alive. If there is no alive probe success in the last timeout" +
        " window, the engine will be marked as no-alive.")
      .version("1.6.0")
      .timeConf
      .createWithDefault(Duration.ofSeconds(120).toMillis)

  val ENGINE_OPEN_MAX_ATTEMPTS: ConfigEntry[Int] =
    buildConf("kyuubi.session.engine.open.max.attempts")
      .doc("The number of times an open engine will retry when encountering a special error.")
      .version("1.7.0")
      .intConf
      .createWithDefault(9)

  val ENGINE_OPEN_RETRY_WAIT: ConfigEntry[Long] =
    buildConf("kyuubi.session.engine.open.retry.wait")
      .doc("How long to wait before retrying to open the engine after failure.")
      .version("1.7.0")
      .timeConf
      .createWithDefault(Duration.ofSeconds(10).toMillis)

  object EngineOpenOnFailure extends Enumeration {
    type EngineOpenOnFailure = Value
    val RETRY, DEREGISTER_IMMEDIATELY, DEREGISTER_AFTER_RETRY = Value
  }

  val ENGINE_OPEN_ON_FAILURE: ConfigEntry[String] =
    buildConf("kyuubi.session.engine.open.onFailure")
      .doc("The behavior when opening engine failed: <ul>" +
        s" <li>RETRY: retry to open engine for ${ENGINE_OPEN_MAX_ATTEMPTS.key} times.</li>" +
        " <li>DEREGISTER_IMMEDIATELY: deregister the engine immediately.</li>" +
        " <li>DEREGISTER_AFTER_RETRY: deregister the engine after retry to open engine for " +
        s"${ENGINE_OPEN_MAX_ATTEMPTS.key} times.</li></ul>")
      .version("1.8.1")
      .stringConf
      .createWithDefault(EngineOpenOnFailure.RETRY.toString)

  val ENGINE_INIT_TIMEOUT: ConfigEntry[Long] = buildConf("kyuubi.session.engine.initialize.timeout")
    .doc("Timeout for starting the background engine, e.g. SparkSQLEngine.")
    .version("1.0.0")
    .timeConf
    .createWithDefault(Duration.ofSeconds(180).toMillis)

  val SESSION_CHECK_INTERVAL: ConfigEntry[Long] = buildConf("kyuubi.session.check.interval")
    .doc("The check interval for session timeout.")
    .version("1.0.0")
    .timeConf
    .checkValue(_ > Duration.ofSeconds(3).toMillis, "Minimum 3 seconds")
    .createWithDefault(Duration.ofMinutes(5).toMillis)

  @deprecated("using kyuubi.session.idle.timeout instead", "1.2.0")
  val SESSION_TIMEOUT: ConfigEntry[Long] = buildConf("kyuubi.session.timeout")
    .doc("(deprecated)session timeout, it will be closed when it's not accessed for this duration")
    .version("1.0.0")
    .timeConf
    .checkValue(_ >= Duration.ofSeconds(3).toMillis, "Minimum 3 seconds")
    .createWithDefault(Duration.ofHours(6).toMillis)

  val SESSION_IDLE_TIMEOUT: ConfigEntry[Long] = buildConf("kyuubi.session.idle.timeout")
    .doc("session idle timeout, it will be closed when it's not accessed for this duration")
    .version("1.2.0")
    .fallbackConf(SESSION_TIMEOUT)

  val SESSION_CLOSE_ON_DISCONNECT: ConfigEntry[Boolean] =
    buildConf("kyuubi.session.close.on.disconnect")
      .doc("Session will be closed when client disconnects from kyuubi gateway. " +
        "Set this to false to have session outlive its parent connection.")
      .version("1.8.0")
      .booleanConf
      .createWithDefault(true)

  val BATCH_SESSION_IDLE_TIMEOUT: ConfigEntry[Long] = buildConf("kyuubi.batch.session.idle.timeout")
    .doc("Batch session idle timeout, it will be closed when it's not accessed for this duration")
    .version("1.6.2")
    .fallbackConf(SESSION_IDLE_TIMEOUT)

  val ENGINE_CHECK_INTERVAL: ConfigEntry[Long] = buildConf("kyuubi.session.engine.check.interval")
    .doc("The check interval for engine timeout")
    .version("1.0.0")
    .timeConf
    .checkValue(_ >= Duration.ofSeconds(1).toMillis, "Minimum 1 seconds")
    .createWithDefault(Duration.ofMinutes(1).toMillis)

  val ENGINE_IDLE_TIMEOUT: ConfigEntry[Long] = buildConf("kyuubi.session.engine.idle.timeout")
    .doc("engine timeout, the engine will self-terminate when it's not accessed for this " +
      "duration. 0 or negative means not to self-terminate.")
    .version("1.0.0")
    .timeConf
    .createWithDefault(Duration.ofMinutes(30L).toMillis)

  val SESSION_CONF_IGNORE_LIST: ConfigEntry[Set[String]] =
    buildConf("kyuubi.session.conf.ignore.list")
      .doc("A comma-separated list of ignored keys. If the client connection contains any of" +
        " them, the key and the corresponding value will be removed silently during engine" +
        " bootstrap and connection setup." +
        " Note that this rule is for server-side protection defined via administrators to" +
        " prevent some essential configs from tampering but will not forbid users to set dynamic" +
        " configurations via SET syntax.")
      .version("1.2.0")
      .stringConf
      .toSet()
      .createWithDefault(Set.empty)

  val SESSION_CONF_RESTRICT_LIST: ConfigEntry[Set[String]] =
    buildConf("kyuubi.session.conf.restrict.list")
      .doc("A comma-separated list of restricted keys. If the client connection contains any of" +
        " them, the connection will be rejected explicitly during engine bootstrap and connection" +
        " setup." +
        " Note that this rule is for server-side protection defined via administrators to" +
        " prevent some essential configs from tampering but will not forbid users to set dynamic" +
        " configurations via SET syntax.")
      .version("1.2.0")
      .stringConf
      .toSet()
      .createWithDefault(Set.empty)

  val SESSION_USER_SIGN_ENABLED: ConfigEntry[Boolean] =
    buildConf("kyuubi.session.user.sign.enabled")
      .doc("Whether to verify the integrity of session user name" +
        " on the engine side, e.g. Authz plugin in Spark.")
      .version("1.7.0")
      .booleanConf
      .createWithDefault(false)

  val SESSION_ENGINE_STARTUP_MAX_LOG_LINES: ConfigEntry[Int] =
    buildConf("kyuubi.session.engine.startup.maxLogLines")
      .doc("The maximum number of engine log lines when errors occur during the engine" +
        " startup phase. Note that this config effects on client-side to" +
        " help track engine startup issues.")
      .version("1.4.0")
      .intConf
      .checkValue(_ > 0, "the maximum must be positive integer.")
      .createWithDefault(10)

  val SESSION_ENGINE_STARTUP_WAIT_COMPLETION: ConfigEntry[Boolean] =
    buildConf("kyuubi.session.engine.startup.waitCompletion")
      .doc("Whether to wait for completion after the engine starts." +
        " If false, the startup process will be destroyed after the engine is started." +
        " Note that only use it when the driver is not running locally," +
        " such as in yarn-cluster mode; Otherwise, the engine will be killed.")
      .version("1.5.0")
      .booleanConf
      .createWithDefault(true)

  val SESSION_ENGINE_STARTUP_DESTROY_TIMEOUT: ConfigEntry[Long] =
    buildConf("kyuubi.session.engine.startup.destroy.timeout")
      .doc("Engine startup process destroy wait time, if the process does not " +
        "stop after this time, force destroy instead. This configuration only " +
        s"takes effect when `${SESSION_ENGINE_STARTUP_WAIT_COMPLETION.key}=false`.")
      .version("1.8.0")
      .timeConf
      .createWithDefault(Duration.ofSeconds(5).toMillis)

  val SESSION_ENGINE_LAUNCH_ASYNC: ConfigEntry[Boolean] =
    buildConf("kyuubi.session.engine.launch.async")
      .doc("When opening kyuubi session, whether to launch the backend engine asynchronously." +
        " When true, the Kyuubi server will set up the connection with the client without delay" +
        " as the backend engine will be created asynchronously.")
      .version("1.4.0")
      .booleanConf
      .createWithDefault(true)

  val SESSION_LOCAL_DIR_ALLOW_LIST: ConfigEntry[Set[String]] =
    buildConf("kyuubi.session.local.dir.allow.list")
      .doc("The local dir list that are allowed to access by the kyuubi session application. " +
        " End-users might set some parameters such as `spark.files` and it will " +
        " upload some local files when launching the kyuubi engine," +
        " if the local dir allow list is defined, kyuubi will" +
        " check whether the path to upload is in the allow list. Note that, if it is empty, there" +
        " is no limitation for that. And please use absolute paths.")
      .version("1.6.0")
      .serverOnly
      .stringConf
      .checkValue(dir => dir.startsWith(File.separator), "the dir should be absolute path")
      .transform(dir => dir.stripSuffix(File.separator) + File.separator)
      .toSet()
      .createWithDefault(Set.empty)

  val BATCH_APPLICATION_CHECK_INTERVAL: ConfigEntry[Long] =
    buildConf("kyuubi.batch.application.check.interval")
      .doc("The interval to check batch job application information.")
      .version("1.6.0")
      .timeConf
      .createWithDefaultString("PT5S")

  val BATCH_APPLICATION_STARVATION_TIMEOUT: ConfigEntry[Long] =
    buildConf("kyuubi.batch.application.starvation.timeout")
      .doc("Threshold above which to warn batch application may be starved.")
      .version("1.7.0")
      .timeConf
      .createWithDefault(Duration.ofMinutes(3).toMillis)

  val BATCH_CONF_IGNORE_LIST: ConfigEntry[Set[String]] =
    buildConf("kyuubi.batch.conf.ignore.list")
      .doc("A comma-separated list of ignored keys for batch conf. If the batch conf contains" +
        " any of them, the key and the corresponding value will be removed silently during batch" +
        " job submission." +
        " Note that this rule is for server-side protection defined via administrators to" +
        " prevent some essential configs from tampering." +
        " You can also pre-define some config for batch job submission with the prefix:" +
        " kyuubi.batchConf.[batchType]. For example, you can pre-define `spark.master`" +
        " for the Spark batch job with key `kyuubi.batchConf.spark.spark.master`.")
      .version("1.6.0")
      .stringConf
      .toSet()
      .createWithDefault(Set.empty)

  val BATCH_INTERNAL_REST_CLIENT_SOCKET_TIMEOUT: ConfigEntry[Long] =
    buildConf("kyuubi.batch.internal.rest.client.socket.timeout")
      .internal
      .doc("The internal rest client socket timeout used for batch request redirection across" +
        " Kyuubi instances.")
      .timeConf
      .createWithDefault(Duration.ofSeconds(20).toMillis)

  val BATCH_INTERNAL_REST_CLIENT_CONNECT_TIMEOUT: ConfigEntry[Long] =
    buildConf("kyuubi.batch.internal.rest.client.connect.timeout")
      .internal
      .doc("The internal rest client connect timeout used for batch request redirection across" +
        " Kyuubi instances.")
      .timeConf
      .createWithDefault(Duration.ofSeconds(20).toMillis)

  val BATCH_INTERNAL_REST_CLIENT_REQUEST_MAX_ATTEMPTS: ConfigEntry[Int] =
    buildConf("kyuubi.batch.internal.rest.client.request.max.attempts")
      .internal
      .doc("The internal rest client max attempts number for batch request redirection across" +
        " Kyuubi instances.")
      .version("1.10.0")
      .intConf
      .createWithDefault(3)

  val BATCH_INTERNAL_REST_CLIENT_REQUEST_ATTEMPT_WAIT: ConfigEntry[Long] =
    buildConf("kyuubi.batch.internal.rest.client.request.attempt.wait")
      .internal
      .doc(
        "The internal rest client wait time between attempts for batch request redirection " +
          "across Kyuubi instances.")
      .version("1.10.0")
      .timeConf
      .createWithDefault(Duration.ofSeconds(3).toMillis)

  val BATCH_CHECK_INTERVAL: ConfigEntry[Long] =
    buildConf("kyuubi.batch.check.interval")
      .internal
      .doc("The interval to check the batch session state. For batch session, it is not" +
        " stateless, and some operations, such as close batch session, must be processed in the" +
        " local kyuubi instance. But sometimes, the kyuubi instance might be unreachable, we" +
        " need mark the batch session be CLOSED state in remote kyuubi instance. And the kyuubi" +
        " instance should check whether there are local batch session are marked as CLOSED" +
        " by remote kyuubi instance and close them periodically.")
      .timeConf
      .createWithDefault(Duration.ofSeconds(5).toMillis)

  val BATCH_RESOURCE_UPLOAD_ENABLED: ConfigEntry[Boolean] =
    buildConf("kyuubi.batch.resource.upload.enabled")
      .internal
      .doc("Whether to enable Kyuubi batch resource upload function.")
      .version("1.7.1")
      .booleanConf
      .createWithDefault(true)

  val BATCH_RESOURCE_FILE_MAX_SIZE: ConfigEntry[Long] =
    buildConf("kyuubi.batch.resource.file.max.size")
      .doc("The maximum size in bytes of the uploaded resource file" +
        " when creating batch. 0 or negative value means no limit.")
      .version("1.10.0")
      .serverOnly
      .longConf
      .createWithDefault(0)

  val BATCH_EXTRA_RESOURCE_FILE_MAX_SIZE: ConfigEntry[Long] =
    buildConf("kyuubi.batch.extra.resource.file.max.size")
      .doc("The maximum size in bytes of each uploaded extra resource file" +
        " when creating batch. 0 or negative value means no limit.")
      .version("1.10.0")
      .serverOnly
      .longConf
      .createWithDefault(0)

  val BATCH_SUBMITTER_ENABLED: ConfigEntry[Boolean] =
    buildConf("kyuubi.batch.submitter.enabled")
      .internal
      .serverOnly
      .doc("Batch API v2 requires batch submitter to pick the INITIALIZED batch job " +
        "from metastore and submits it to Resource Manager. " +
        "Note: Batch API v2 is experimental and under rapid development, this configuration " +
        "is added to allow explorers conveniently testing the developing Batch v2 API, not " +
        "intended exposing to end users, it may be removed in anytime.")
      .version("1.8.0")
      .booleanConf
      .createWithDefault(false)

  val BATCH_SUBMITTER_THREADS: ConfigEntry[Int] =
    buildConf("kyuubi.batch.submitter.threads")
      .internal
      .serverOnly
      .doc("Number of threads in batch job submitter, this configuration only take effects " +
        s"when ${BATCH_SUBMITTER_ENABLED.key} is enabled")
      .version("1.8.0")
      .intConf
      .createWithDefault(16)

  val BATCH_IMPL_VERSION: ConfigEntry[String] =
    buildConf("kyuubi.batch.impl.version")
      .internal
      .serverOnly
      .doc("Batch API version, candidates: 1, 2. Only take effect when " +
        s"${BATCH_SUBMITTER_ENABLED.key} is true, otherwise always use v1 implementation. " +
        "Note: Batch API v2 is experimental and under rapid development, this configuration " +
        "is added to allow explorers conveniently testing the developing Batch v2 API, not " +
        "intended exposing to end users, it may be removed in anytime.")
      .version("1.8.0")
      .stringConf
      .createWithDefault("1")

  val SERVER_EXEC_POOL_SIZE: ConfigEntry[Int] =
    buildConf("kyuubi.backend.server.exec.pool.size")
      .doc("Number of threads in the operation execution thread pool of Kyuubi server")
      .version("1.0.0")
      .intConf
      .createWithDefault(100)

  val ENGINE_EXEC_POOL_SIZE: ConfigEntry[Int] =
    buildConf("kyuubi.backend.engine.exec.pool.size")
      .doc("Number of threads in the operation execution thread pool of SQL engine applications")
      .version("1.0.0")
      .fallbackConf(SERVER_EXEC_POOL_SIZE)

  val SERVER_EXEC_WAIT_QUEUE_SIZE: ConfigEntry[Int] =
    buildConf("kyuubi.backend.server.exec.pool.wait.queue.size")
      .doc("Size of the wait queue for the operation execution thread pool of Kyuubi server")
      .version("1.0.0")
      .intConf
      .createWithDefault(100)

  val METADATA_STORE_CLASS: ConfigEntry[String] =
    buildConf("kyuubi.metadata.store.class")
      .doc("Fully qualified class name for server metadata store.")
      .version("1.6.0")
      .stringConf
      .createWithDefault("org.apache.kyuubi.server.metadata.jdbc.JDBCMetadataStore")

  val METADATA_CLEANER_ENABLED: ConfigEntry[Boolean] =
    buildConf("kyuubi.metadata.cleaner.enabled")
      .doc("Whether to clean the metadata periodically. If it is enabled, Kyuubi will clean the" +
        " metadata that is in the terminate state with max age limitation.")
      .version("1.6.0")
      .booleanConf
      .createWithDefault(true)

  val METADATA_MAX_AGE: ConfigEntry[Long] =
    buildConf("kyuubi.metadata.max.age")
      .doc("The maximum age of metadata, the metadata exceeding the age will be cleaned.")
      .version("1.6.0")
      .timeConf
      .createWithDefault(Duration.ofDays(3).toMillis)

  val METADATA_CLEANER_INTERVAL: ConfigEntry[Long] =
    buildConf("kyuubi.metadata.cleaner.interval")
      .doc("The interval to check and clean expired metadata.")
      .version("1.6.0")
      .timeConf
      .createWithDefault(Duration.ofMinutes(30).toMillis)

  val METADATA_RECOVERY_THREADS: ConfigEntry[Int] =
    buildConf("kyuubi.metadata.recovery.threads")
      .doc("The number of threads for recovery from the metadata store " +
        "when the Kyuubi server restarts.")
      .version("1.6.0")
      .intConf
      .createWithDefault(10)

  val METADATA_REQUEST_RETRY_INTERVAL: ConfigEntry[Long] =
    buildConf("kyuubi.metadata.request.retry.interval")
      .doc("The interval to check and trigger the metadata request retry tasks.")
      .version("1.6.0")
      .timeConf
      .createWithDefault(Duration.ofSeconds(5).toMillis)

  val METADATA_REQUEST_ASYNC_RETRY_ENABLED: ConfigEntry[Boolean] =
    buildConf("kyuubi.metadata.request.async.retry.enabled")
      .doc("Whether to retry in async when metadata request failed. When true, return " +
        "success response immediately even the metadata request failed, and schedule " +
        "it in background until success, to tolerate long-time metadata store outages " +
        "w/o blocking the submission request.")
      .version("1.7.0")
      .booleanConf
      .createWithDefault(true)

  val METADATA_REQUEST_ASYNC_RETRY_THREADS: ConfigEntry[Int] =
    buildConf("kyuubi.metadata.request.async.retry.threads")
      .withAlternative("kyuubi.metadata.request.retry.threads")
      .doc("Number of threads in the metadata request async retry manager thread pool. Only " +
        s"take affect when ${METADATA_REQUEST_ASYNC_RETRY_ENABLED.key} is `true`.")
      .version("1.6.0")
      .intConf
      .createWithDefault(10)

  val METADATA_REQUEST_ASYNC_RETRY_QUEUE_SIZE: ConfigEntry[Int] =
    buildConf("kyuubi.metadata.request.async.retry.queue.size")
      .withAlternative("kyuubi.metadata.request.retry.queue.size")
      .doc("The maximum queue size for buffering metadata requests in memory when the external" +
        " metadata storage is down. Requests will be dropped if the queue exceeds. Only" +
        s" take affect when ${METADATA_REQUEST_ASYNC_RETRY_ENABLED.key} is `true`.")
      .version("1.6.0")
      .intConf
      .createWithDefault(65536)

  val METADATA_SEARCH_WINDOW: OptionalConfigEntry[Long] =
    buildConf("kyuubi.metadata.search.window")
      .doc("The time window to restrict user queries to metadata within a specific period, " +
        "starting from the current time to the past. It only affects `GET /api/v1/batches` API. " +
        "You may want to set this to short period to improve query performance and reduce load " +
        "on the metadata store when administer want to reserve the metadata for long time. " +
        "The side-effects is that, the metadata created outside the window will not be " +
        "invisible to users. If it is undefined, all metadata will be visible for users.")
      .version("1.10.1")
      .timeConf
      .checkValue(_ > 0, "must be positive number")
      .createOptional

  val ENGINE_EXEC_WAIT_QUEUE_SIZE: ConfigEntry[Int] =
    buildConf("kyuubi.backend.engine.exec.pool.wait.queue.size")
      .doc("Size of the wait queue for the operation execution thread pool in SQL engine" +
        " applications")
      .version("1.0.0")
      .fallbackConf(SERVER_EXEC_WAIT_QUEUE_SIZE)

  val SERVER_EXEC_KEEPALIVE_TIME: ConfigEntry[Long] =
    buildConf("kyuubi.backend.server.exec.pool.keepalive.time")
      .doc("Time(ms) that an idle async thread of the operation execution thread pool will wait" +
        " for a new task to arrive before terminating in Kyuubi server")
      .version("1.0.0")
      .timeConf
      .createWithDefault(Duration.ofSeconds(60).toMillis)

  val ENGINE_EXEC_KEEPALIVE_TIME: ConfigEntry[Long] =
    buildConf("kyuubi.backend.engine.exec.pool.keepalive.time")
      .doc("Time(ms) that an idle async thread of the operation execution thread pool will wait" +
        " for a new task to arrive before terminating in SQL engine applications")
      .version("1.0.0")
      .fallbackConf(SERVER_EXEC_KEEPALIVE_TIME)

  val SERVER_EXEC_POOL_SHUTDOWN_TIMEOUT: ConfigEntry[Long] =
    buildConf("kyuubi.backend.server.exec.pool.shutdown.timeout")
      .doc("Timeout(ms) for the operation execution thread pool to terminate in Kyuubi server")
      .version("1.0.0")
      .timeConf
      .createWithDefault(Duration.ofSeconds(10).toMillis)

  val ENGINE_EXEC_POOL_SHUTDOWN_TIMEOUT: ConfigEntry[Long] =
    buildConf("kyuubi.backend.engine.exec.pool.shutdown.timeout")
      .doc("Timeout(ms) for the operation execution thread pool to terminate in SQL engine" +
        " applications")
      .version("1.0.0")
      .fallbackConf(SERVER_EXEC_POOL_SHUTDOWN_TIMEOUT)

  val OPERATION_STATUS_POLLING_TIMEOUT: ConfigEntry[Long] =
    buildConf("kyuubi.operation.status.polling.timeout")
      .doc("Timeout(ms) for long polling asynchronous running sql query's status")
      .version("1.0.0")
      .timeConf
      .createWithDefault(Duration.ofSeconds(5).toMillis)

  val OPERATION_STATUS_UPDATE_INTERVAL: ConfigEntry[Long] =
    buildConf("kyuubi.operation.status.update.interval")
      .internal
      .doc("Interval(ms) for updating the same status for a query.")
      .version("1.7.0")
      .timeConf
      .createWithDefault(Duration.ofSeconds(5).toMillis)

  val OPERATION_FORCE_CANCEL: ConfigEntry[Boolean] =
    buildConf("kyuubi.operation.interrupt.on.cancel")
      .doc("When true, all running tasks will be interrupted if one cancels a query. " +
        "When false, all running tasks will remain until finished.")
      .version("1.2.0")
      .booleanConf
      .createWithDefault(true)

  val OPERATION_QUERY_TIMEOUT: OptionalConfigEntry[Long] =
    buildConf("kyuubi.operation.query.timeout")
      .doc("Timeout for query executions at server-side, take effect with client-side timeout(" +
        "`java.sql.Statement.setQueryTimeout`) together, a running query will be cancelled" +
        " automatically if timeout. It's off by default, which means only client-side take full" +
        " control of whether the query should timeout or not." +
        " If set, client-side timeout is capped at this point." +
        " To cancel the queries right away without waiting for task to finish," +
        s" consider enabling ${OPERATION_FORCE_CANCEL.key} together.")
      .version("1.2.0")
      .timeConf
      .checkValue(_ >= 1000, "must >= 1s if set")
      .createOptional

  val OPERATION_QUERY_TIMEOUT_MONITOR_ENABLED: ConfigEntry[Boolean] =
    buildConf("kyuubi.operation.query.timeout.monitor.enabled")
      .doc("Whether to monitor timeout query timeout check on server side.")
      .version("1.8.0")
      .serverOnly
      .internal
      .booleanConf
      .createWithDefault(true)

  val OPERATION_RESULT_MAX_ROWS: ConfigEntry[Int] =
    buildConf("kyuubi.operation.result.max.rows")
      .doc("Max rows of Spark query results. Rows exceeding the limit would be ignored. " +
        "By setting this value to 0 to disable the max rows limit.")
      .version("1.6.0")
      .intConf
      .createWithDefault(0)

  val OPERATION_RESULT_SAVE_TO_FILE: ConfigEntry[Boolean] =
    buildConf("kyuubi.operation.result.saveToFile.enabled")
      .doc("The switch for Spark query result save to file.")
      .version("1.9.0")
      .booleanConf
      .createWithDefault(false)

  val OPERATION_RESULT_SAVE_TO_FILE_DIR: ConfigEntry[String] =
    buildConf("kyuubi.operation.result.saveToFile.dir")
      .doc("The Spark query result save dir, it should be a public accessible to every engine." +
        " Results are saved in ORC format, and the directory structure is" +
        " `/OPERATION_RESULT_SAVE_TO_FILE_DIR/engineId/sessionId/statementId`." +
        " Each query result will delete when query finished.")
      .version("1.9.0")
      .stringConf
      .createWithDefault("/tmp/kyuubi/tmp_kyuubi_result")

  val OPERATION_RESULT_SAVE_TO_FILE_MINSIZE: ConfigEntry[Long] =
    buildConf("kyuubi.operation.result.saveToFile.minSize")
      .doc("The minSize of Spark result save to file, default value is 200 MB." +
        "we use spark's `EstimationUtils#getSizePerRowestimate` to estimate" +
        " the output size of the execution plan.")
      .version("1.9.0")
      .longConf
      .checkValue(_ > 0, "must be positive value")
      .createWithDefault(200 * 1024 * 1024)

  val OPERATION_RESULT_SAVE_TO_FILE_MIN_ROWS: ConfigEntry[Long] =
    buildConf("kyuubi.operation.result.saveToFile.minRows")
      .doc("The minRows of Spark result save to file, default value is 10000.")
      .version("1.9.1")
      .longConf
      .checkValue(_ > 0, "must be positive value")
      .createWithDefault(10000)

  val OPERATION_INCREMENTAL_COLLECT: ConfigEntry[Boolean] =
    buildConf("kyuubi.operation.incremental.collect")
      .internal
      .doc("When true, the executor side result will be sequentially calculated and returned to" +
        s" the engine side.")
      .version("1.4.0")
      .booleanConf
      .createWithDefault(false)

  val OPERATION_RESULT_FORMAT: ConfigEntry[String] =
    buildConf("kyuubi.operation.result.format")
      .doc("Specify the result format, available configs are: <ul>" +
        " <li>THRIFT: the result will convert to TRow at the engine driver side. </li>" +
        " <li>ARROW: the result will be encoded as Arrow at the executor side before collecting" +
        " by the driver, and deserialized at the client side. note that it only takes effect for" +
        " kyuubi-hive-jdbc clients now.</li></ul>")
      .version("1.7.0")
      .stringConf
      .checkValues(Set("arrow", "thrift"))
      .transformToLowerCase
      .createWithDefault("thrift")

  val ARROW_BASED_ROWSET_TIMESTAMP_AS_STRING: ConfigEntry[Boolean] =
    buildConf("kyuubi.operation.result.arrow.timestampAsString")
      .doc("When true, arrow-based rowsets will convert columns of type timestamp to strings for" +
        " transmission.")
      .version("1.7.0")
      .booleanConf
      .createWithDefault(false)

  val SERVER_OPERATION_LOG_DIR_ROOT: ConfigEntry[String] =
    buildConf("kyuubi.operation.log.dir.root")
      .doc("Root directory for query operation log at server-side.")
      .version("1.4.0")
      .serverOnly
      .stringConf
      .createWithDefault("server_operation_logs")

  val PROXY_USER: OptionalConfigEntry[String] =
    buildConf("kyuubi.session.proxy.user")
      .doc("An alternative to hive.server2.proxy.user. " +
        "The current behavior is consistent with hive.server2.proxy.user " +
        "and now only takes effect in RESTFul API. " +
        "When both parameters are set, kyuubi.session.proxy.user takes precedence.")
      .version("1.9.0")
      .stringConf
      .createOptional

  @deprecated("using kyuubi.engine.share.level instead", "1.2.0")
  val LEGACY_ENGINE_SHARE_LEVEL: ConfigEntry[String] =
    buildConf("kyuubi.session.engine.share.level")
      .doc(s"(deprecated) - Using kyuubi.engine.share.level instead")
      .version("1.0.0")
      .stringConf
      .transformToUpperCase
      .checkValues(ShareLevel)
      .createWithDefault(ShareLevel.USER.toString)

  // [ZooKeeper Data Model]
  // (http://zookeeper.apache.org/doc/r3.7.0/zookeeperProgrammers.html#ch_zkDataModel)
  private val validZookeeperSubPath: Pattern = ("(?!^[\\u002e]{1,2}$)" +
    "(^[\\u0020-\\u002e\\u0030-\\u007e\\u00a0-\\ud7ff\\uf900-\\uffef]{1,}$)").r.pattern

  @deprecated("using kyuubi.engine.share.level.subdomain instead", "1.4.0")
  val ENGINE_SHARE_LEVEL_SUB_DOMAIN: ConfigEntry[Option[String]] =
    buildConf("kyuubi.engine.share.level.sub.domain")
      .doc("(deprecated) - Using kyuubi.engine.share.level.subdomain instead")
      .version("1.2.0")
      .stringConf
      .transformToLowerCase
      .checkValue(validZookeeperSubPath.matcher(_).matches(), "must be valid zookeeper sub path.")
      .createOptional

  val ENGINE_SHARE_LEVEL_SUBDOMAIN: ConfigEntry[Option[String]] =
    buildConf("kyuubi.engine.share.level.subdomain")
      .doc("Allow end-users to create a subdomain for the share level of an engine. A" +
        " subdomain is a case-insensitive string values that must be a valid zookeeper subpath." +
        " For example, for the `USER` share level, an end-user can share a certain engine within" +
        " a subdomain, not for all of its clients. End-users are free to create multiple" +
        " engines in the `USER` share level. When disable engine pool, use 'default' if absent.")
      .version("1.4.0")
      .fallbackConf(ENGINE_SHARE_LEVEL_SUB_DOMAIN)

  @deprecated("using kyuubi.frontend.connection.url.use.hostname instead, 1.5.0")
  val ENGINE_CONNECTION_URL_USE_HOSTNAME: ConfigEntry[Boolean] =
    buildConf("kyuubi.engine.connection.url.use.hostname")
      .doc("(deprecated) " +
        "When true, the engine registers with hostname to zookeeper. When Spark runs on K8s" +
        " with cluster mode, set to false to ensure that server can connect to engine")
      .version("1.3.0")
      .booleanConf
      .createWithDefault(true)

  val FRONTEND_CONNECTION_URL_USE_HOSTNAME: ConfigEntry[Boolean] =
    buildConf("kyuubi.frontend.connection.url.use.hostname")
      .doc("When true, frontend services prefer hostname, otherwise, ip address. Note that, " +
        "the default value is set to `false` when engine running on Kubernetes to prevent " +
        "potential network issues.")
      .version("1.5.0")
      .fallbackConf(ENGINE_CONNECTION_URL_USE_HOSTNAME)

  val ENGINE_DO_AS_ENABLED: ConfigEntry[Boolean] =
    buildConf("kyuubi.engine.doAs.enabled")
      .doc("Whether to enable user impersonation on launching engine. When enabled, " +
        "for engines which supports user impersonation, e.g. SPARK, depends on the " +
        s"`kyuubi.engine.share.level`, different users will be used to launch the engine. " +
        "Otherwise, Kyuubi Server's user will always be used to launch the engine.")
      .version("1.9.0")
      .booleanConf
      .createWithDefault(true)

  val ENGINE_SHARE_LEVEL: ConfigEntry[String] = buildConf("kyuubi.engine.share.level")
    .doc("Engines will be shared in different levels, available configs are: <ul>" +
      " <li>CONNECTION: the engine will not be shared but only used by the current client" +
      " connection, and the engine will be launched by session user.</li>" +
      " <li>USER: the engine will be shared by all sessions created by a unique username," +
      s" and the engine will be launched by session user.</li>" +
      " <li>GROUP: the engine will be shared by all sessions created" +
      " by all users belong to the same primary group name." +
      " The engine will be launched by the primary group name as the effective" +
      " username, so here the group name is in value of special user who is able to visit the" +
      " computing resources/data of the team. It follows the" +
      " [Hadoop GroupsMapping](https://reurl.cc/xE61Y5) to map user to a primary group. If the" +
      " primary group is not found, it fallback to the USER level." +
      " <li>SERVER: the engine will be shared by Kyuubi servers, and the engine will be launched" +
      " by Server's user.</li>" +
      " </ul>" +
      s" See also `${ENGINE_SHARE_LEVEL_SUBDOMAIN.key}` and `${ENGINE_DO_AS_ENABLED.key}`.")
    .version("1.2.0")
    .fallbackConf(LEGACY_ENGINE_SHARE_LEVEL)

  val ENGINE_TYPE: ConfigEntry[String] = buildConf("kyuubi.engine.type")
    .doc("Specify the detailed engine supported by Kyuubi. The engine type bindings to" +
      " SESSION scope. This configuration is experimental. Currently, available configs are: <ul>" +
      " <li>SPARK_SQL: specify this engine type will launch a Spark engine which can provide" +
      " all the capacity of the Apache Spark. Note, it's a default engine type.</li>" +
      " <li>FLINK_SQL: specify this engine type will launch a Flink engine which can provide" +
      " all the capacity of the Apache Flink.</li>" +
      " <li>TRINO: specify this engine type will launch a Trino engine which can provide" +
      " all the capacity of the Trino.</li>" +
      " <li>HIVE_SQL: specify this engine type will launch a Hive engine which can provide" +
      " all the capacity of the Hive Server2.</li>" +
      " <li>JDBC: specify this engine type will launch a JDBC engine which can forward" +
      " queries to the database system through the certain JDBC driver," +
      " for now, it supports Doris, MySQL, Phoenix, PostgreSQL, StarRocks, Impala" +
      " and ClickHouse.</li>" +
      " <li>CHAT: specify this engine type will launch a Chat engine.</li>" +
      "</ul>")
    .version("1.4.0")
    .stringConf
    .transformToUpperCase
    .checkValues(EngineType)
    .createWithDefault(EngineType.SPARK_SQL.toString)

  val ENGINE_POOL_IGNORE_SUBDOMAIN: ConfigEntry[Boolean] =
    buildConf("kyuubi.engine.pool.ignoreSubdomain")
      .doc(s"Whether to ignore ${ENGINE_SHARE_LEVEL_SUBDOMAIN.key}" +
        s" when engine pool conditions met.")
      .internal
      .version("1.7.0")
      .booleanConf
      .createWithDefault(false)

  val ENGINE_POOL_NAME: ConfigEntry[String] = buildConf("kyuubi.engine.pool.name")
    .doc("The name of the engine pool.")
    .version("1.5.0")
    .stringConf
    .checkValue(validZookeeperSubPath.matcher(_).matches(), "must be valid zookeeper sub path.")
    .createWithDefault("engine-pool")

  val ENGINE_POOL_SIZE_THRESHOLD: ConfigEntry[Int] = buildConf("kyuubi.engine.pool.size.threshold")
    .doc("This parameter is introduced as a server-side parameter " +
      "controlling the upper limit of the engine pool.")
    .version("1.4.0")
    .serverOnly
    .intConf
    .checkValue(s => s > 0 && s < 33, "Invalid engine pool threshold, it should be in [1, 32]")
    .createWithDefault(9)

  val ENGINE_POOL_SIZE: ConfigEntry[Int] = buildConf("kyuubi.engine.pool.size")
    .doc("The size of the engine pool. Note that, " +
      "if the size is less than 1, the engine pool will not be enabled; " +
      "otherwise, the size of the engine pool will be " +
      s"min(this, ${ENGINE_POOL_SIZE_THRESHOLD.key}).")
    .version("1.4.0")
    .intConf
    .createWithDefault(-1)

  val ENGINE_POOL_SELECT_POLICY: ConfigEntry[String] =
    buildConf("kyuubi.engine.pool.selectPolicy")
      .doc("The select policy of an engine from the corresponding engine pool engine for " +
        "a session. <ul>" +
        "<li>RANDOM - Randomly use the engine in the pool</li>" +
        "<li>POLLING - Polling use the engine in the pool</li>" +
        "</ul>")
      .version("1.7.0")
      .stringConf
      .transformToUpperCase
      .checkValues(Set("RANDOM", "POLLING"))
      .createWithDefault("RANDOM")

  val ENGINE_INITIALIZE_SQL: ConfigEntry[Seq[String]] =
    buildConf("kyuubi.engine.initialize.sql")
      .doc("SemiColon-separated list of SQL statements to be initialized in the newly created " +
        "engine before queries. i.e. use `SHOW DATABASES` to eagerly active HiveClient. This " +
        "configuration can not be used in JDBC url due to the limitation of Beeline/JDBC driver.")
      .version("1.2.0")
      .stringConf
      .toSequence(";")
      .createWithDefaultString("SHOW DATABASES")

  val ENGINE_SESSION_INITIALIZE_SQL: ConfigEntry[Seq[String]] =
    buildConf("kyuubi.engine.session.initialize.sql")
      .doc("SemiColon-separated list of SQL statements to be initialized in the newly created " +
        "engine session before queries. This configuration can not be used in JDBC url due to " +
        "the limitation of Beeline/JDBC driver.")
      .version("1.3.0")
      .stringConf
      .toSequence(";")
      .createWithDefault(Nil)

  val ENGINE_SESSION_FLINK_INITIALIZE_SQL: ConfigEntry[Seq[String]] =
    buildConf("kyuubi.session.engine.flink.initialize.sql")
      .doc("The initialize sql for Flink session. " +
        "It fallback to `kyuubi.engine.session.initialize.sql`")
      .version("1.8.1")
      .fallbackConf(ENGINE_SESSION_INITIALIZE_SQL)

  val ENGINE_DEREGISTER_EXCEPTION_CLASSES: ConfigEntry[Set[String]] =
    buildConf("kyuubi.engine.deregister.exception.classes")
      .doc("A comma-separated list of exception classes. If there is any exception thrown," +
        " whose class matches the specified classes, the engine would deregister itself.")
      .version("1.2.0")
      .stringConf
      .toSet()
      .createWithDefault(Set.empty)

  val ENGINE_DEREGISTER_EXCEPTION_MESSAGES: ConfigEntry[Set[String]] =
    buildConf("kyuubi.engine.deregister.exception.messages")
      .doc("A comma-separated list of exception messages. If there is any exception thrown," +
        " whose message or stacktrace matches the specified message list, the engine would" +
        " deregister itself.")
      .version("1.2.0")
      .stringConf
      .toSet()
      .createWithDefault(Set.empty)

  val ENGINE_DEREGISTER_JOB_MAX_FAILURES: ConfigEntry[Int] =
    buildConf("kyuubi.engine.deregister.job.max.failures")
      .doc("Number of failures of job before deregistering the engine.")
      .version("1.2.0")
      .intConf
      .checkValue(_ > 0, "must be positive number")
      .createWithDefault(4)

  val ENGINE_DEREGISTER_EXCEPTION_TTL: ConfigEntry[Long] =
    buildConf("kyuubi.engine.deregister.exception.ttl")
      .doc(s"Time to live(TTL) for exceptions pattern specified in" +
        s" ${ENGINE_DEREGISTER_EXCEPTION_CLASSES.key} and" +
        s" ${ENGINE_DEREGISTER_EXCEPTION_MESSAGES.key} to deregister engines. Once the total" +
        s" error count hits the ${ENGINE_DEREGISTER_JOB_MAX_FAILURES.key} within the TTL, an" +
        s" engine will deregister itself and wait for self-terminated. Otherwise, we suppose" +
        s" that the engine has recovered from temporary failures.")
      .version("1.2.0")
      .timeConf
      .checkValue(_ > 0, "must be positive number")
      .createWithDefault(Duration.ofMinutes(30).toMillis)

  val OPERATION_SCHEDULER_POOL: OptionalConfigEntry[String] =
    buildConf("kyuubi.operation.scheduler.pool")
      .doc("The scheduler pool of job. Note that, this config should be used after changing " +
        "Spark config spark.scheduler.mode=FAIR.")
      .version("1.1.1")
      .stringConf
      .createOptional

  val ENGINE_SINGLE_SPARK_SESSION: ConfigEntry[Boolean] =
    buildConf("kyuubi.engine.single.spark.session")
      .doc("When set to true, this engine is running in a single session mode. " +
        "All the JDBC/ODBC connections share the temporary views, function registries, " +
        "SQL configuration and the current database.")
      .version("1.3.0")
      .booleanConf
      .createWithDefault(false)

  val ENGINE_USER_ISOLATED_SPARK_SESSION: ConfigEntry[Boolean] =
    buildConf("kyuubi.engine.user.isolated.spark.session")
      .doc("When set to false, if the engine is running in a group or server share level, " +
        "all the JDBC/ODBC connections will be isolated against the user. Including " +
        "the temporary views, function registries, SQL configuration, and the current database. " +
        "Note that, it does not affect if the share level is connection or user.")
      .version("1.6.0")
      .booleanConf
      .createWithDefault(true)

  val ENGINE_USER_ISOLATED_SPARK_SESSION_IDLE_TIMEOUT: ConfigEntry[Long] =
    buildConf("kyuubi.engine.user.isolated.spark.session.idle.timeout")
      .doc(s"If ${ENGINE_USER_ISOLATED_SPARK_SESSION.key} is false, we will release the " +
        s"Spark session if its corresponding user is inactive after this configured timeout.")
      .version("1.6.0")
      .timeConf
      .createWithDefault(Duration.ofHours(6).toMillis)

  val ENGINE_USER_ISOLATED_SPARK_SESSION_IDLE_INTERVAL: ConfigEntry[Long] =
    buildConf("kyuubi.engine.user.isolated.spark.session.idle.interval")
      .doc(s"The interval to check if the user-isolated Spark session is timeout.")
      .version("1.6.0")
      .timeConf
      .createWithDefault(Duration.ofMinutes(1).toMillis)

  val SERVER_EVENT_JSON_LOG_PATH: ConfigEntry[String] =
    buildConf("kyuubi.backend.server.event.json.log.path")
      .doc("The location of server events go for the built-in JSON logger")
      .version("1.4.0")
      .serverOnly
      .stringConf
      .createWithDefault("file:///tmp/kyuubi/events")

  val ENGINE_EVENT_JSON_LOG_PATH: ConfigEntry[String] =
    buildConf("kyuubi.engine.event.json.log.path")
      .doc("The location where all the engine events go for the built-in JSON logger.<ul>" +
        "<li>Local Path: start with 'file://'</li>" +
        "<li>HDFS Path: start with 'hdfs://'</li></ul>")
      .version("1.3.0")
      .stringConf
      .createWithDefault("file:///tmp/kyuubi/events")

  val SERVER_EVENT_KAFKA_TOPIC: OptionalConfigEntry[String] =
    buildConf("kyuubi.backend.server.event.kafka.topic")
      .doc("The topic of server events go for the built-in Kafka logger")
      .version("1.8.0")
      .serverOnly
      .stringConf
      .createOptional

  val SERVER_EVENT_KAFKA_CLOSE_TIMEOUT: ConfigEntry[Long] =
    buildConf("kyuubi.backend.server.event.kafka.close.timeout")
      .doc("Period to wait for Kafka producer of server event handlers to close.")
      .version("1.8.0")
      .serverOnly
      .timeConf
      .createWithDefault(Duration.ofMillis(5000).toMillis)

  val SERVER_EVENT_LOGGERS: ConfigEntry[Seq[String]] =
    buildConf("kyuubi.backend.server.event.loggers")
      .doc("A comma-separated list of server history loggers, where session/operation etc" +
        " events go.<ul>" +
        s" <li>JSON: the events will be written to the location of" +
        s" ${SERVER_EVENT_JSON_LOG_PATH.key}</li>" +
        s" <li>KAFKA: the events will be serialized in JSON format" +
        s" and sent to topic of `${SERVER_EVENT_KAFKA_TOPIC.key}`." +
        s" Note: For the configs of Kafka producer," +
        s" please specify them with the prefix: `kyuubi.backend.server.event.kafka.`." +
        s" For example, `kyuubi.backend.server.event.kafka.bootstrap.servers=127.0.0.1:9092`" +
        s" </li>" +
        s" <li>JDBC: to be done</li>" +
        s" <li>CUSTOM: User-defined event handlers.</li></ul>" +
        " Note that: Kyuubi supports custom event handlers with the Java SPI." +
        " To register a custom event handler," +
        " the user needs to implement a class" +
        " which is a child of org.apache.kyuubi.events.handler.CustomEventHandlerProvider" +
        " which has a zero-arg constructor.")
      .version("1.4.0")
      .serverOnly
      .stringConf
      .transformToUpperCase
      .toSequence()
      .checkValue(
        _.toSet.subsetOf(Set("JSON", "JDBC", "CUSTOM", "KAFKA")),
        "Unsupported event loggers")
      .createWithDefault(Nil)

  @deprecated("using kyuubi.engine.spark.event.loggers instead", "1.6.0")
  val ENGINE_EVENT_LOGGERS: ConfigEntry[Seq[String]] =
    buildConf("kyuubi.engine.event.loggers")
      .doc("A comma-separated list of engine history loggers, where engine/session/operation etc" +
        " events go.<ul>" +
        " <li>SPARK: the events will be written to the Spark listener bus.</li>" +
        " <li>JSON: the events will be written to the location of" +
        s" ${ENGINE_EVENT_JSON_LOG_PATH.key}</li>" +
        " <li>JDBC: to be done</li>" +
        " <li>CUSTOM: User-defined event handlers.</li></ul>" +
        " Note that: Kyuubi supports custom event handlers with the Java SPI." +
        " To register a custom event handler," +
        " the user needs to implement a subclass" +
        " of `org.apache.kyuubi.events.handler.CustomEventHandlerProvider`" +
        " which has a zero-arg constructor.")
      .version("1.3.0")
      .stringConf
      .transformToUpperCase
      .toSequence()
      .checkValue(
        _.toSet.subsetOf(Set("SPARK", "JSON", "JDBC", "CUSTOM")),
        "Unsupported event loggers")
      .createWithDefault(Seq("SPARK"))

  val ENGINE_UI_STOP_ENABLED: ConfigEntry[Boolean] =
    buildConf("kyuubi.engine.ui.stop.enabled")
      .doc("When true, allows Kyuubi engine to be killed from the Spark Web UI.")
      .version("1.3.0")
      .booleanConf
      .createWithDefault(true)

  val ENGINE_UI_SESSION_LIMIT: ConfigEntry[Int] =
    buildConf("kyuubi.engine.ui.retainedSessions")
      .doc("The number of SQL client sessions kept in the Kyuubi Query Engine web UI.")
      .version("1.4.0")
      .intConf
      .checkValue(_ > 0, "retained sessions must be positive.")
      .createWithDefault(200)

  val ENGINE_UI_STATEMENT_LIMIT: ConfigEntry[Int] =
    buildConf("kyuubi.engine.ui.retainedStatements")
      .doc("The number of statements kept in the Kyuubi Query Engine web UI.")
      .version("1.4.0")
      .intConf
      .checkValue(_ > 0, "retained statements must be positive.")
      .createWithDefault(200)

  val ENGINE_OPERATION_LOG_DIR_ROOT: ConfigEntry[String] =
    buildConf("kyuubi.engine.operation.log.dir.root")
      .doc("Root directory for query operation log at engine-side.")
      .version("1.4.0")
      .stringConf
      .createWithDefault("engine_operation_logs")

  val ENGINE_SECURITY_ENABLED: ConfigEntry[Boolean] =
    buildConf("kyuubi.engine.security.enabled")
      .internal
      .doc("Whether to enable the internal secure access. Before 1.6.0, it is used for the secure" +
        " access between kyuubi server and kyuubi engine. Since 1.6.0, kyuubi supports internal" +
        " secure across kyuubi server instances.")
      .version("1.5.0")
      .booleanConf
      .createWithDefault(false)

  val ENGINE_SECURITY_TOKEN_MAX_LIFETIME: ConfigEntry[Long] =
    buildConf("kyuubi.engine.security.token.max.lifetime")
      .internal
      .doc("The max lifetime of the token used for internal secure access.")
      .version("1.5.0")
      .timeConf
      .createWithDefault(Duration.ofMinutes(10).toMillis)

  val ENGINE_SECURITY_SECRET_PROVIDER: ConfigEntry[String] =
    buildConf("kyuubi.engine.security.secret.provider")
      .internal
      .doc("The class used to manage the internal security secret. This class must be a " +
        "subclass of `EngineSecuritySecretProvider`.")
      .version("1.5.0")
      .stringConf
      .transform {
        case "simple" =>
          "org.apache.kyuubi.service.authentication.SimpleEngineSecuritySecretProviderImpl"
        case "zookeeper" =>
          "org.apache.kyuubi.service.authentication.ZooKeeperEngineSecuritySecretProviderImpl"
        case other => other
      }
      .createWithDefault("zookeeper")

  val SIMPLE_SECURITY_SECRET_PROVIDER_PROVIDER_SECRET: OptionalConfigEntry[String] =
    buildConf("kyuubi.engine.security.secret.provider.simple.secret")
      .internal
      .doc("The secret key used for internal security access. Only take affects when " +
        s"${ENGINE_SECURITY_SECRET_PROVIDER.key} is 'simple'")
      .version("1.7.0")
      .stringConf
      .createOptional

  val ENGINE_SECURITY_CRYPTO_KEY_LENGTH: ConfigEntry[Int] =
    buildConf("kyuubi.engine.security.crypto.keyLength")
      .internal
      .doc("The length in bits of the encryption key to generate. " +
        "Valid values are 128, 192 and 256")
      .version("1.5.0")
      .intConf
      .checkValues(Set(128, 192, 256))
      .createWithDefault(128)

  val ENGINE_SECURITY_CRYPTO_IV_LENGTH: ConfigEntry[Int] =
    buildConf("kyuubi.engine.security.crypto.ivLength")
      .internal
      .doc("Initial vector length, in bytes.")
      .version("1.5.0")
      .intConf
      .createWithDefault(16)

  val ENGINE_SECURITY_CRYPTO_KEY_ALGORITHM: ConfigEntry[String] =
    buildConf("kyuubi.engine.security.crypto.keyAlgorithm")
      .internal
      .doc("The algorithm for generated secret keys.")
      .version("1.5.0")
      .stringConf
      .createWithDefault("AES")

  val ENGINE_SECURITY_CRYPTO_CIPHER_TRANSFORMATION: ConfigEntry[String] =
    buildConf("kyuubi.engine.security.crypto.cipher")
      .internal
      .doc("The cipher transformation to use for encrypting internal access token.")
      .version("1.5.0")
      .stringConf
      .createWithDefault("AES/CBC/PKCS5PADDING")

  val SESSION_NAME: OptionalConfigEntry[String] =
    buildConf("kyuubi.session.name")
      .doc("A human readable name of the session and we use empty string by default. " +
        "This name will be recorded in the event. Note that, we only apply this value from " +
        "session conf.")
      .version("1.4.0")
      .stringConf
      .createOptional

  val OPERATION_PLAN_ONLY_MODE: ConfigEntry[String] =
    buildConf("kyuubi.operation.plan.only.mode")
      .doc("Configures the statement performed mode, The value can be 'parse', 'analyze', " +
        "'optimize', 'optimize_with_stats', 'physical', 'execution', 'lineage' or 'none', " +
        "when it is 'none', indicate to the statement will be fully executed, otherwise " +
        "only way without executing the query. different engines currently support different " +
        "modes, the Spark engine supports all modes, and the Flink engine supports 'parse', " +
        "'physical', and 'execution', other engines do not support planOnly currently.")
      .version("1.4.0")
      .stringConf
      .transformToUpperCase
      .checkValue(
        mode =>
          Set(
            "PARSE",
            "ANALYZE",
            "OPTIMIZE",
            "OPTIMIZE_WITH_STATS",
            "PHYSICAL",
            "EXECUTION",
            "LINEAGE",
            "NONE").contains(mode),
        "Invalid value for 'kyuubi.operation.plan.only.mode'. Valid values are" +
          "'parse', 'analyze', 'optimize', 'optimize_with_stats', 'physical', 'execution' and " +
          "'lineage', 'none'.")
      .createWithDefault(NoneMode.name)

  val OPERATION_PLAN_ONLY_OUT_STYLE: ConfigEntry[String] =
    buildConf("kyuubi.operation.plan.only.output.style")
      .doc("Configures the planOnly output style. The value can be 'plain' or 'json', and " +
        "the default value is 'plain'. This configuration supports only the output styles " +
        "of the Spark engine")
      .version("1.7.0")
      .stringConf
      .transformToUpperCase
      .checkValues(Set("PLAIN", "JSON"))
      .createWithDefault(PlainStyle.name)

  val OPERATION_PLAN_ONLY_EXCLUDES: ConfigEntry[Set[String]] =
    buildConf("kyuubi.operation.plan.only.excludes")
      .doc("Comma-separated list of query plan names, in the form of simple class names, i.e, " +
        "for `SET abc=xyz`, the value will be `SetCommand`. For those auxiliary plans, such as " +
        "`switch databases`, `set properties`, or `create temporary view` etc., " +
        "which are used for setup evaluating environments for analyzing actual queries, " +
        "we can use this config to exclude them and let them take effect. " +
        s"See also ${OPERATION_PLAN_ONLY_MODE.key}.")
      .version("1.5.0")
      .stringConf
      .toSet()
      .createWithDefault(Set(
        "ResetCommand",
        "SetCommand",
        "SetNamespaceCommand",
        "UseStatement",
        "SetCatalogAndNamespace"))

  val LINEAGE_PARSER_PLUGIN_PROVIDER: ConfigEntry[String] =
    buildConf("kyuubi.lineage.parser.plugin.provider")
      .doc("The provider for the Spark lineage parser plugin.")
      .version("1.8.0")
      .stringConf
      .createWithDefault("org.apache.kyuubi.plugin.lineage.LineageParserProvider")

  object OperationLanguages extends Enumeration with Logging {
    type OperationLanguage = Value
    val PYTHON, SQL, SCALA, UNKNOWN = Value
    def apply(language: String): OperationLanguage = {
      language.toUpperCase(Locale.ROOT) match {
        case "PYTHON" => PYTHON
        case "SQL" => SQL
        case "SCALA" => SCALA
        case other =>
          warn(s"Unsupported operation language: $language, using UNKNOWN instead")
          UNKNOWN
      }
    }
  }

  val OPERATION_LANGUAGE: ConfigEntry[String] =
    buildConf("kyuubi.operation.language")
      .doc("Choose a programing language for the following inputs" +
        "<ul>" +
        "<li>SQL: (Default) Run all following statements as SQL queries.</li>" +
        "<li>SCALA: Run all following input as scala codes</li>" +
        "<li>PYTHON: (Experimental) Run all following input as Python codes with Spark engine" +
        "</li>" +
        "</ul>")
      .version("1.5.0")
      .stringConf
      .transformToUpperCase
      .checkValues(OperationLanguages)
      .createWithDefault(OperationLanguages.SQL.toString)

  val SESSION_CONF_ADVISOR: OptionalConfigEntry[Seq[String]] =
    buildConf("kyuubi.session.conf.advisor")
      .doc("A config advisor plugin for Kyuubi Server. This plugin can provide a list of custom " +
        "configs for different users or session configs and overwrite the session configs before " +
        "opening a new session. This config value should be a subclass of " +
        "`org.apache.kyuubi.plugin.SessionConfAdvisor` which has a zero-arg constructor.")
      .version("1.5.0")
      .stringConf
      .toSequence()
      .createOptional

  val GROUP_PROVIDER: ConfigEntry[String] =
    buildConf("kyuubi.session.group.provider")
      .doc("A group provider plugin for Kyuubi Server. This plugin can provide primary group " +
        "and groups information for different users or session configs. This config value " +
        "should be a subclass of `org.apache.kyuubi.plugin.GroupProvider` which " +
        "has a zero-arg constructor. Kyuubi provides the following built-in implementations: " +
        "<li>hadoop: delegate the user group mapping to hadoop UserGroupInformation.</li>")
      .version("1.7.0")
      .stringConf
      .transform {
        case "hadoop" => "org.apache.kyuubi.session.HadoopGroupProvider"
        case other => other
      }
      .createWithDefault("hadoop")

  val SERVER_NAME: OptionalConfigEntry[String] =
    buildConf("kyuubi.server.name")
      .doc("The name of Kyuubi Server.")
      .version("1.5.0")
      .serverOnly
      .stringConf
      .createOptional

  val SERVER_INFO_PROVIDER: ConfigEntry[String] =
    buildConf("kyuubi.server.info.provider")
      .doc("The server information provider name, some clients may rely on this information" +
        " to check the server compatibilities and functionalities." +
        " <li>SERVER: Return Kyuubi server information.</li>" +
        " <li>ENGINE: Return Kyuubi engine information.</li>")
      .version("1.6.1")
      .stringConf
      .createWithDefault("ENGINE")

  val ENGINE_SPARK_SHOW_PROGRESS: ConfigEntry[Boolean] =
    buildConf("kyuubi.session.engine.spark.showProgress")
      .doc("When true, show the progress bar in the Spark's engine log.")
      .version("1.6.0")
      .booleanConf
      .createWithDefault(false)

  val ENGINE_SPARK_SHOW_PROGRESS_UPDATE_INTERVAL: ConfigEntry[Long] =
    buildConf("kyuubi.session.engine.spark.progress.update.interval")
      .doc("Update period of progress bar.")
      .version("1.6.0")
      .timeConf
      .checkValue(_ >= 200, "Minimum 200 milliseconds")
      .createWithDefault(1000)

  val ENGINE_SPARK_SHOW_PROGRESS_TIME_FORMAT: ConfigEntry[String] =
    buildConf("kyuubi.session.engine.spark.progress.timeFormat")
      .doc("The time format of the progress bar")
      .version("1.6.0")
      .stringConf
      .createWithDefault("yyyy-MM-dd HH:mm:ss.SSS")

  val ENGINE_SPARK_OPERATION_INCREMENTAL_COLLECT: ConfigEntry[Boolean] =
    buildConf("kyuubi.engine.spark.operation.incremental.collect")
      .doc("When true, the result will be sequentially calculated and returned to" +
        s" the Spark driver. Note that, ${OPERATION_RESULT_MAX_ROWS.key} will be ignored" +
        " on incremental collect mode. It fallback to `kyuubi.operation.incremental.collect`")
      .version("1.10.0")
      .fallbackConf(OPERATION_INCREMENTAL_COLLECT)

  val ENGINE_SPARK_OPERATION_INCREMENTAL_COLLECT_CANCEL_JOB_GROUP: ConfigEntry[Boolean] =
    buildConf(
      "kyuubi.engine.spark.operation.incremental.collect.cancelJobGroupAfterExecutionFinished")
      .internal
      .doc("Canceling jobs group that are still running after statement execution finished " +
        "avoids wasting resources. But the cancellation may cause the query fail when using " +
        "incremental collect mode.")
      .version("1.9.2")
      .booleanConf
      .createWithDefault(false)

  val ENGINE_SESSION_SPARK_INITIALIZE_SQL: ConfigEntry[Seq[String]] =
    buildConf("kyuubi.session.engine.spark.initialize.sql")
      .doc("The initialize sql for Spark session. " +
        "It fallback to `kyuubi.engine.session.initialize.sql`")
      .version("1.8.1")
      .fallbackConf(ENGINE_SESSION_INITIALIZE_SQL)

  val ENGINE_TRINO_MEMORY: ConfigEntry[String] =
    buildConf("kyuubi.engine.trino.memory")
      .doc("The heap memory for the Trino query engine")
      .version("1.6.0")
      .stringConf
      .createWithDefault("1g")

  val ENGINE_TRINO_JAVA_OPTIONS: OptionalConfigEntry[String] =
    buildConf("kyuubi.engine.trino.java.options")
      .doc("The extra Java options for the Trino query engine")
      .version("1.6.0")
      .stringConf
      .createOptional

  val ENGINE_TRINO_EXTRA_CLASSPATH: OptionalConfigEntry[String] =
    buildConf("kyuubi.engine.trino.extra.classpath")
      .doc("The extra classpath for the Trino query engine, " +
        "for configuring other libs which may need by the Trino engine ")
      .version("1.6.0")
      .stringConf
      .createOptional

  val ENGINE_TRINO_OPERATION_INCREMENTAL_COLLECT: ConfigEntry[Boolean] =
    buildConf("kyuubi.engine.trino.operation.incremental.collect")
      .doc("When true, the result will be sequentially calculated and returned to" +
        " the trino. It fallback to `kyuubi.operation.incremental.collect`")
      .version("1.10.0")
      .fallbackConf(OPERATION_INCREMENTAL_COLLECT)

  val ENGINE_HIVE_MEMORY: ConfigEntry[String] =
    buildConf("kyuubi.engine.hive.memory")
      .doc("The heap memory for the Hive query engine")
      .version("1.6.0")
      .stringConf
      .createWithDefault("1g")

  val ENGINE_HIVE_JAVA_OPTIONS: OptionalConfigEntry[String] =
    buildConf("kyuubi.engine.hive.java.options")
      .doc("The extra Java options for the Hive query engine")
      .version("1.6.0")
      .stringConf
      .createOptional

  val ENGINE_HIVE_EXTRA_CLASSPATH: OptionalConfigEntry[String] =
    buildConf("kyuubi.engine.hive.extra.classpath")
      .doc("The extra classpath for the Hive query engine, for configuring location" +
        " of the hadoop client jars and etc.")
      .version("1.6.0")
      .stringConf
      .createOptional

  val ENGINE_HIVE_DEPLOY_MODE: ConfigEntry[String] =
    buildConf("kyuubi.engine.hive.deploy.mode")
      .doc("Configures the hive engine deploy mode, The value can be 'local', 'yarn'. " +
        "In local mode, the engine operates on the same node as the KyuubiServer. " +
        "In YARN mode, the engine runs within the Application Master (AM) container of YARN. ")
      .version("1.9.0")
      .stringConf
      .transformToUpperCase
      .checkValue(
        mode => Set("LOCAL", "YARN").contains(mode),
        "Invalid value for 'kyuubi.engine.hive.deploy.mode'. Valid values are 'local', 'yarn'.")
      .createWithDefault(DeployMode.LOCAL.toString)

  val ENGINE_DEPLOY_YARN_MODE_STAGING_DIR: OptionalConfigEntry[String] =
    buildConf("kyuubi.engine.yarn.stagingDir")
      .doc("Staging directory used while submitting kyuubi engine to YARN, " +
        "It should be a absolute path in HDFS.")
      .version("1.9.0")
      .stringConf
      .createOptional

  val ENGINE_DEPLOY_YARN_MODE_TAGS: OptionalConfigEntry[Seq[String]] =
    buildConf("kyuubi.engine.yarn.tags")
      .doc(s"kyuubi engine yarn tags when the engine deploy mode is YARN.")
      .version("1.9.0")
      .stringConf
      .toSequence()
      .createOptional

  val ENGINE_DEPLOY_YARN_MODE_QUEUE: ConfigEntry[String] =
    buildConf("kyuubi.engine.yarn.queue")
      .doc(s"kyuubi engine yarn queue when the engine deploy mode is YARN.")
      .version("1.9.0")
      .stringConf
      .createWithDefault("default")

  val ENGINE_DEPLOY_YARN_MODE_PRIORITY: OptionalConfigEntry[Int] =
    buildConf("kyuubi.engine.yarn.priority")
      .doc(s"kyuubi engine yarn priority when the engine deploy mode is YARN.")
      .version("1.9.0")
      .intConf
      .createOptional

  val ENGINE_DEPLOY_YARN_MODE_APP_NAME: OptionalConfigEntry[String] =
    buildConf("kyuubi.engine.yarn.app.name")
      .doc(s"The YARN app name when the engine deploy mode is YARN.")
      .version("1.9.0")
      .stringConf
      .createOptional

  val ENGINE_DEPLOY_YARN_MODE_MEMORY: ConfigEntry[Int] =
    buildConf("kyuubi.engine.yarn.memory")
      .doc(s"kyuubi engine container memory in mb when the engine deploy mode is YARN.")
      .version("1.9.0")
      .intConf
      .createWithDefault(1024)

  val ENGINE_DEPLOY_YARN_MODE_CORES: ConfigEntry[Int] =
    buildConf("kyuubi.engine.yarn.cores")
      .doc(s"kyuubi engine container core number when the engine deploy mode is YARN.")
      .version("1.9.0")
      .intConf
      .createWithDefault(1)

  val ENGINE_DEPLOY_YARN_MODE_JAVA_OPTIONS: OptionalConfigEntry[String] =
    buildConf("kyuubi.engine.yarn.java.options")
      .doc(s"The extra Java options for the AM when the engine deploy mode is YARN.")
      .version("1.9.0")
      .stringConf
      .createOptional

  val ENGINE_PRINCIPAL: OptionalConfigEntry[String] =
    buildConf("kyuubi.engine.principal")
      .doc("Kerberos principal for the kyuubi engine.")
      .version("1.10.0")
      .stringConf
      .createOptional

  val ENGINE_KEYTAB: OptionalConfigEntry[String] =
    buildConf("kyuubi.engine.keytab")
      .doc("Kerberos keytab for the kyuubi engine.")
      .version("1.10.0")
      .stringConf
      .createOptional

  val ENGINE_FLINK_MEMORY: ConfigEntry[String] =
    buildConf("kyuubi.engine.flink.memory")
      .doc("The heap memory for the Flink SQL engine. Only effective in yarn session mode.")
      .version("1.6.0")
      .stringConf
      .createWithDefault("1g")

  val ENGINE_FLINK_JAVA_OPTIONS: OptionalConfigEntry[String] =
    buildConf("kyuubi.engine.flink.java.options")
      .doc("The extra Java options for the Flink SQL engine. Only effective in yarn session mode.")
      .version("1.6.0")
      .stringConf
      .createOptional

  val ENGINE_FLINK_EXTRA_CLASSPATH: OptionalConfigEntry[String] =
    buildConf("kyuubi.engine.flink.extra.classpath")
      .doc("The extra classpath for the Flink SQL engine, for configuring the location" +
        " of hadoop client jars, etc. Only effective in yarn session mode.")
      .version("1.6.0")
      .stringConf
      .createOptional

  val ENGINE_FLINK_APPLICATION_JARS: OptionalConfigEntry[String] =
    buildConf("kyuubi.engine.flink.application.jars")
      .doc("A comma-separated list of the local jars to be shipped with the job to the cluster. " +
        "For example, SQL UDF jars. Only effective in yarn application mode.")
      .version("1.8.0")
      .stringConf
      .createOptional

  val ENGINE_FLINK_INITIALIZE_SQL: ConfigEntry[Seq[String]] =
    buildConf("kyuubi.engine.flink.initialize.sql")
      .doc("The initialize sql for Flink engine. It fallback to `kyuubi.engine.initialize.sql`.")
      .version("1.8.1")
      .fallbackConf(ENGINE_INITIALIZE_SQL)

  val ENGINE_FLINK_DOAS_ENABLED: ConfigEntry[Boolean] =
    buildConf("kyuubi.engine.flink.doAs.enabled")
      .doc("When enabled, the session user is used as the proxy user to launch the Flink engine," +
        " otherwise, the server user. Note, due to the limitation of Apache Flink," +
        " it can only be enabled on Kerberized environment.")
      .version("1.10.0")
      .booleanConf
      .createWithDefault(false)

  val ENGINE_FLINK_DOAS_GENERATE_TOKEN_FILE: ConfigEntry[Boolean] =
    buildConf("kyuubi.engine.flink.doAs.generateTokenFile")
      .internal
      .doc(s"When ${ENGINE_FLINK_DOAS_ENABLED.key}=true and neither FLINK-35525 (Flink 1.20.0)" +
        " nor YARN-10333 (Hadoop 3.4.0) is available, enable this configuration to generate a" +
        " temporary HADOOP_TOKEN_FILE that will be picked up by the Flink engine bootstrap" +
        " process.")
      .version("1.10.0")
      .booleanConf
      .createWithDefault(false)

  val SERVER_LIMIT_CONNECTIONS_PER_USER: OptionalConfigEntry[Int] =
    buildConf("kyuubi.server.limit.connections.per.user")
      .doc("Maximum kyuubi server connections per user." +
        " Any user exceeding this limit will not be allowed to connect.")
      .version("1.6.0")
      .serverOnly
      .intConf
      .createOptional

  val SERVER_LIMIT_CONNECTIONS_PER_IPADDRESS: OptionalConfigEntry[Int] =
    buildConf("kyuubi.server.limit.connections.per.ipaddress")
      .doc("Maximum kyuubi server connections per ipaddress." +
        " Any user exceeding this limit will not be allowed to connect.")
      .version("1.6.0")
      .serverOnly
      .intConf
      .createOptional

  val SERVER_LIMIT_CONNECTIONS_PER_USER_IPADDRESS: OptionalConfigEntry[Int] =
    buildConf("kyuubi.server.limit.connections.per.user.ipaddress")
      .doc("Maximum kyuubi server connections per user:ipaddress combination." +
        " Any user-ipaddress exceeding this limit will not be allowed to connect.")
      .version("1.6.0")
      .serverOnly
      .intConf
      .createOptional

  val SERVER_LIMIT_CONNECTIONS_USER_UNLIMITED_LIST: ConfigEntry[Set[String]] =
    buildConf("kyuubi.server.limit.connections.user.unlimited.list")
      .doc("The maximum connections of the user in the white list will not be limited.")
      .version("1.7.0")
      .serverOnly
      .stringConf
      .toSet()
      .createWithDefault(Set.empty)

  val SERVER_LIMIT_CONNECTIONS_USER_DENY_LIST: ConfigEntry[Set[String]] =
    buildConf("kyuubi.server.limit.connections.user.deny.list")
      .doc("The user in the deny list will be denied to connect to kyuubi server, " +
        "if the user has configured both user.unlimited.list and user.deny.list, " +
        "the priority of the latter is higher.")
      .version("1.8.0")
      .serverOnly
      .stringConf
      .toSet()
      .createWithDefault(Set.empty)

  val SERVER_LIMIT_CONNECTIONS_IP_DENY_LIST: ConfigEntry[Set[String]] =
    buildConf("kyuubi.server.limit.connections.ip.deny.list")
      .doc("The client ip in the deny list will be denied to connect to kyuubi server.")
      .version("1.9.1")
      .serverOnly
      .stringConf
      .toSet()
      .createWithDefault(Set.empty)

  val SERVER_LIMIT_BATCH_CONNECTIONS_PER_USER: OptionalConfigEntry[Int] =
    buildConf("kyuubi.server.limit.batch.connections.per.user")
      .doc("Maximum kyuubi server batch connections per user." +
        " Any user exceeding this limit will not be allowed to connect.")
      .version("1.7.0")
      .serverOnly
      .intConf
      .createOptional

  val SERVER_LIMIT_BATCH_CONNECTIONS_PER_IPADDRESS: OptionalConfigEntry[Int] =
    buildConf("kyuubi.server.limit.batch.connections.per.ipaddress")
      .doc("Maximum kyuubi server batch connections per ipaddress." +
        " Any user exceeding this limit will not be allowed to connect.")
      .version("1.7.0")
      .serverOnly
      .intConf
      .createOptional

  val SERVER_LIMIT_BATCH_CONNECTIONS_PER_USER_IPADDRESS: OptionalConfigEntry[Int] =
    buildConf("kyuubi.server.limit.batch.connections.per.user.ipaddress")
      .doc("Maximum kyuubi server batch connections per user:ipaddress combination." +
        " Any user-ipaddress exceeding this limit will not be allowed to connect.")
      .version("1.7.0")
      .serverOnly
      .intConf
      .createOptional

  val SERVER_LIMIT_CLIENT_FETCH_MAX_ROWS: OptionalConfigEntry[Int] =
    buildConf("kyuubi.server.limit.client.fetch.max.rows")
      .doc("Max rows limit for getting result row set operation. If the max rows specified " +
        "by client-side is larger than the limit, request will fail directly.")
      .version("1.8.0")
      .serverOnly
      .intConf
      .createOptional

  val SESSION_PROGRESS_ENABLE: ConfigEntry[Boolean] =
    buildConf("kyuubi.operation.progress.enabled")
      .doc("Whether to enable the operation progress. When true," +
        " the operation progress will be returned in `GetOperationStatus`.")
      .version("1.6.0")
      .booleanConf
      .createWithDefault(false)

  val SERVER_SECRET_REDACTION_PATTERN: OptionalConfigEntry[Regex] =
    buildConf("kyuubi.server.redaction.regex")
      .doc("Regex to decide which Kyuubi contain sensitive information. When this regex matches " +
        "a property key or value, the value is redacted from the various logs.")
      .version("1.6.0")
      .regexConf
      .createOptional

  val SERVER_PERIODIC_GC_INTERVAL: ConfigEntry[Long] =
    buildConf("kyuubi.server.periodicGC.interval")
      .doc("How often to trigger the periodic garbage collection. 0 will disable it.")
      .version("1.7.0")
      .serverOnly
      .timeConf
      .createWithDefaultString("PT30M")

  val SERVER_TEMP_FILE_EXPIRE_TIME: ConfigEntry[Long] =
    buildConf("kyuubi.server.tempFile.expireTime")
      .doc("Expiration timout for cleanup server-side temporary files, e.g. operation logs.")
      .version("1.10.0")
      .serverOnly
      .timeConf
      .createWithDefaultString("P30D")

  val SERVER_TEMP_FILE_EXPIRE_MAX_COUNT: OptionalConfigEntry[Int] =
    buildConf("kyuubi.server.tempFile.maxCount")
      .doc("The upper threshold size of server-side temporary file paths to cleanup")
      .version("1.10.0")
      .serverOnly
      .intConf
      .createOptional

  val SERVER_ADMINISTRATORS: ConfigEntry[Set[String]] =
    buildConf("kyuubi.server.administrators")
      .doc("Comma-separated list of Kyuubi service administrators. " +
        "We use this config to grant admin permission to any service accounts when " +
        s"security mechanism is enabled. Note, when ${AUTHENTICATION_METHOD.key} is " +
        "configured to NOSASL or NONE, everyone is treated as administrator.")
      .version("1.8.0")
      .serverOnly
      .stringConf
      .toSet()
      .createWithDefault(Set.empty)

  val OPERATION_SPARK_LISTENER_ENABLED: ConfigEntry[Boolean] =
    buildConf("kyuubi.operation.spark.listener.enabled")
      .doc("When set to true, Spark engine registers an SQLOperationListener before executing " +
        "the statement, logging a few summary statistics when each stage completes.")
      .version("1.6.0")
      .booleanConf
      .createWithDefault(true)

  val ENGINE_JDBC_DRIVER_CLASS: OptionalConfigEntry[String] =
    buildConf("kyuubi.engine.jdbc.driver.class")
      .doc("The driver class for JDBC engine connection")
      .version("1.6.0")
      .stringConf
      .createOptional

  val ENGINE_JDBC_CONNECTION_URL: OptionalConfigEntry[String] =
    buildConf("kyuubi.engine.jdbc.connection.url")
      .doc("The server url that engine will connect to")
      .version("1.6.0")
      .stringConf
      .createOptional

  val ENGINE_JDBC_CONNECTION_PROPAGATECREDENTIAL: ConfigEntry[Boolean] =
    buildConf("kyuubi.engine.jdbc.connection.propagateCredential")
      .doc("Whether to use the session's user and password to connect to database")
      .version("1.8.0")
      .booleanConf
      .createWithDefault(false)

  val ENGINE_JDBC_CONNECTION_USER: OptionalConfigEntry[String] =
    buildConf("kyuubi.engine.jdbc.connection.user")
      .doc("The user is used for connecting to server")
      .version("1.6.0")
      .stringConf
      .createOptional

  val ENGINE_JDBC_CONNECTION_PASSWORD: OptionalConfigEntry[String] =
    buildConf("kyuubi.engine.jdbc.connection.password")
      .doc("The password is used for connecting to server")
      .version("1.6.0")
      .stringConf
      .createOptional

  val ENGINE_JDBC_CONNECTION_PROPERTIES: ConfigEntry[Seq[String]] =
    buildConf("kyuubi.engine.jdbc.connection.properties")
      .doc("The additional properties are used for connecting to server")
      .version("1.6.0")
      .stringConf
      .toSequence()
      .createWithDefault(Nil)

  val ENGINE_JDBC_CONNECTION_PROVIDER: OptionalConfigEntry[String] =
    buildConf("kyuubi.engine.jdbc.connection.provider")
      .doc("A JDBC connection provider plugin for the Kyuubi Server " +
        "to establish a connection to the JDBC URL." +
        " The configuration value should be a subclass of " +
        "`org.apache.kyuubi.engine.jdbc.connection.JdbcConnectionProvider`. " +
        "Kyuubi provides the following built-in implementations: " +
        "<li>doris: For establishing Doris connections.</li> " +
        "<li>mysql: For establishing MySQL connections.</li> " +
        "<li>phoenix: For establishing Phoenix connections.</li> " +
        "<li>postgresql: For establishing PostgreSQL connections.</li>" +
        "<li>starrocks: For establishing StarRocks connections.</li>" +
        "<li>impala: For establishing Impala connections.</li>" +
        "<li>clickhouse: For establishing clickhouse connections.</li>" +
        "<li>oracle: For establishing oracle connections.</li>")
      .version("1.6.0")
      .stringConf
      .transform {
        case "Doris" | "doris" | "DorisConnectionProvider" =>
          "org.apache.kyuubi.engine.jdbc.doris.DorisConnectionProvider"
        case "MySQL" | "mysql" | "MySQLConnectionProvider" =>
          "org.apache.kyuubi.engine.jdbc.mysql.MySQLConnectionProvider"
        case "Phoenix" | "phoenix" | "PhoenixConnectionProvider" =>
          "org.apache.kyuubi.engine.jdbc.phoenix.PhoenixConnectionProvider"
        case "PostgreSQL" | "postgresql" | "PostgreSQLConnectionProvider" =>
          "org.apache.kyuubi.engine.jdbc.postgresql.PostgreSQLConnectionProvider"
        case "StarRocks" | "starrocks" | "StarRocksConnectionProvider" =>
          "org.apache.kyuubi.engine.jdbc.starrocks.StarRocksConnectionProvider"
        case "Impala" | "impala" | "ImpalaConnectionProvider" =>
          "org.apache.kyuubi.engine.jdbc.impala.ImpalaConnectionProvider"
        case "ClickHouse" | "clickhouse" | "ClickHouseConnectionProvider" =>
          "org.apache.kyuubi.engine.jdbc.clickhouse.ClickHouseConnectionProvider"
        case "Oracle" | "oracle" | "OracleConnectionProvider" =>
          "org.apache.kyuubi.engine.jdbc.oracle.OracleConnectionProvider"
        case other => other
      }
      .createOptional

  val ENGINE_JDBC_SHORT_NAME: OptionalConfigEntry[String] =
    buildConf("kyuubi.engine.jdbc.type")
      .doc("The short name of JDBC type")
      .version("1.6.0")
      .stringConf
      .createOptional

  val ENGINE_JDBC_INITIALIZE_SQL: ConfigEntry[Seq[String]] =
    buildConf("kyuubi.engine.jdbc.initialize.sql")
      .doc("SemiColon-separated list of SQL statements to be initialized in the newly created " +
        "engine before queries. i.e. use `SELECT 1` to eagerly active JDBCClient.")
      .version("1.8.0")
      .stringConf
      .toSequence(";")
      .createWithDefaultString("SELECT 1")

  val ENGINE_JDBC_SESSION_INITIALIZE_SQL: ConfigEntry[Seq[String]] =
    buildConf("kyuubi.engine.jdbc.session.initialize.sql")
      .doc("SemiColon-separated list of SQL statements to be initialized in the newly created " +
        "engine session before queries.")
      .version("1.8.0")
      .stringConf
      .toSequence(";")
      .createWithDefault(Nil)

  val ENGINE_JDBC_FETCH_SIZE: ConfigEntry[Int] =
    buildConf("kyuubi.engine.jdbc.fetch.size")
      .doc("The fetch size of JDBC engine")
      .version("1.9.0")
      .intConf
      .createWithDefault(1000)

  val ENGINE_JDBC_OPERATION_INCREMENTAL_COLLECT: ConfigEntry[Boolean] =
    buildConf("kyuubi.engine.jdbc.operation.incremental.collect")
      .doc("When true, the result will be sequentially calculated and returned to" +
        " the JDBC engine. It fallback to `kyuubi.operation.incremental.collect`")
      .version("1.10.0")
      .fallbackConf(OPERATION_INCREMENTAL_COLLECT)

  val ENGINE_JDBC_DEPLOY_MODE: ConfigEntry[String] =
    buildConf("kyuubi.engine.jdbc.deploy.mode")
      .doc("Configures the jdbc engine deploy mode, The value can be 'local', 'yarn'. " +
        "In local mode, the engine operates on the same node as the KyuubiServer. " +
        "In YARN mode, the engine runs within the Application Master (AM) container of YARN. ")
      .version("1.10.0")
      .stringConf
      .transformToUpperCase
      .checkValue(
        mode => Set("LOCAL", "YARN").contains(mode),
        "Invalid value for 'kyuubi.engine.jdbc.deploy.mode'. Valid values are 'local', 'yarn'.")
      .createWithDefault(DeployMode.LOCAL.toString)

  val ENGINE_OPERATION_CONVERT_CATALOG_DATABASE_ENABLED: ConfigEntry[Boolean] =
    buildConf("kyuubi.engine.operation.convert.catalog.database.enabled")
      .doc("When set to true, The engine converts the JDBC methods of set/get Catalog " +
        "and set/get Schema to the implementation of different engines")
      .version("1.6.0")
      .booleanConf
      .createWithDefault(true)

  val ENGINE_SUBMIT_TIMEOUT: ConfigEntry[Long] =
    buildConf("kyuubi.engine.submit.timeout")
      .doc("Period to tolerant Driver Pod ephemerally invisible after submitting. " +
        "In some Resource Managers, e.g. K8s, the Driver Pod is not visible immediately " +
        "after `spark-submit` is returned.")
      .version("1.7.1")
      .timeConf
      .createWithDefaultString("PT30S")

  val ENGINE_KUBERNETES_SUBMIT_TIMEOUT: ConfigEntry[Long] =
    buildConf("kyuubi.engine.kubernetes.submit.timeout")
      .doc("The engine submit timeout for Kubernetes application.")
      .version("1.7.2")
      .fallbackConf(ENGINE_SUBMIT_TIMEOUT)

  val ENGINE_YARN_SUBMIT_TIMEOUT: ConfigEntry[Long] =
    buildConf("kyuubi.engine.yarn.submit.timeout")
      .doc("The engine submit timeout for YARN application.")
      .version("1.7.2")
      .fallbackConf(ENGINE_SUBMIT_TIMEOUT)

  object YarnUserStrategy extends Enumeration {
    type YarnUserStrategy = Value
    val NONE, ADMIN, OWNER = Value
  }

  val YARN_USER_STRATEGY: ConfigEntry[String] =
    buildConf("kyuubi.yarn.user.strategy")
      .doc("Determine which user to use to construct YARN client for application management, " +
        "e.g. kill application. Options: <ul>" +
        "<li>NONE: use Kyuubi server user.</li>" +
        "<li>ADMIN: use admin user configured in `kyuubi.yarn.user.admin`.</li>" +
        "<li>OWNER: use session user, typically is application owner.</li>" +
        "</ul>")
      .version("1.8.0")
      .stringConf
      .checkValues(YarnUserStrategy)
      .createWithDefault("NONE")

  val YARN_USER_ADMIN: ConfigEntry[String] =
    buildConf("kyuubi.yarn.user.admin")
      .doc(s"When ${YARN_USER_STRATEGY.key} is set to ADMIN, use this admin user to " +
        "construct YARN client for application management, e.g. kill application.")
      .version("1.8.0")
      .stringConf
      .createWithDefault("yarn")

  /**
   * Holds information about keys that have been deprecated.
   *
   * @param key The deprecated key.
   * @param version Version of Kyuubi where key was deprecated.
   * @param comment Additional info regarding to the removed config. For example,
   *                reasons of config deprecation, what users should use instead of it.
   */
  case class DeprecatedConfig(key: String, version: String, comment: String)

  private val deprecatedConfigs: Map[String, DeprecatedConfig] = {
    val configs = Seq(
      DeprecatedConfig(
        FRONTEND_BIND_PORT.key,
        "1.4.0",
        s"Use ${FRONTEND_THRIFT_BINARY_BIND_PORT.key} instead"),
      DeprecatedConfig(
        FRONTEND_MAX_MESSAGE_SIZE.key,
        "1.4.0",
        s"Use ${FRONTEND_THRIFT_MAX_MESSAGE_SIZE.key} instead"),
      DeprecatedConfig(
        SESSION_TIMEOUT.key,
        "1.2.0",
        s"Use ${SESSION_IDLE_TIMEOUT.key} instead"),
      DeprecatedConfig(
        LEGACY_ENGINE_SHARE_LEVEL.key,
        "1.2.0",
        s"Use ${ENGINE_SHARE_LEVEL.key} instead"),
      DeprecatedConfig(
        ENGINE_SHARE_LEVEL_SUB_DOMAIN.key,
        "1.4.0",
        s"Use ${ENGINE_SHARE_LEVEL_SUBDOMAIN.key} instead"),
      DeprecatedConfig(
        ENGINE_CONNECTION_URL_USE_HOSTNAME.key,
        "1.5.0",
        s"Use ${FRONTEND_CONNECTION_URL_USE_HOSTNAME.key} instead"),

      // deprecated configs of [[org.apache.kyuubi.zookeeper.ZookeeperConf]]
      DeprecatedConfig(
        "kyuubi.zookeeper.embedded.port",
        "1.2.0",
        "Use kyuubi.zookeeper.embedded.client.port instead"),
      DeprecatedConfig(
        "kyuubi.zookeeper.embedded.directory",
        "1.2.0",
        "Use kyuubi.zookeeper.embedded.data.dir instead"),

      // deprecated configs of [[org.apache.kyuubi.ha.HighAvailabilityConf]]
      DeprecatedConfig(
        "kyuubi.ha.zookeeper.quorum",
        "1.6.0",
        "Use kyuubi.ha.addresses instead"),
      DeprecatedConfig(
        "kyuubi.ha.zookeeper.namespace",
        "1.6.0",
        "Use kyuubi.ha.namespace instead"),
      DeprecatedConfig(
        "kyuubi.ha.zookeeper.acl.enabled",
        "1.3.2",
        "Use kyuubi.ha.zookeeper.auth.type and kyuubi.ha.zookeeper.engine.auth.type instead"))
    Map(configs.map { cfg => cfg.key -> cfg }: _*)
  }

  val ENGINE_CHAT_MEMORY: ConfigEntry[String] =
    buildConf("kyuubi.engine.chat.memory")
      .doc("The heap memory for the Chat engine")
      .version("1.8.0")
      .stringConf
      .createWithDefault("1g")

  val ENGINE_CHAT_JAVA_OPTIONS: OptionalConfigEntry[String] =
    buildConf("kyuubi.engine.chat.java.options")
      .doc("The extra Java options for the Chat engine")
      .version("1.8.0")
      .stringConf
      .createOptional

  val ENGINE_CHAT_PROVIDER: ConfigEntry[String] =
    buildConf("kyuubi.engine.chat.provider")
      .doc("The provider for the Chat engine. Candidates: <ul>" +
        " <li>ECHO: simply replies a welcome message.</li>" +
        " <li>GPT: a.k.a ChatGPT, powered by OpenAI.</li>" +
        " <li>ERNIE: ErnieBot, powered by Baidu.</li>" +
        "</ul>")
      .version("1.8.0")
      .stringConf
      .transform {
        case "ECHO" | "echo" => "org.apache.kyuubi.engine.chat.provider.EchoProvider"
        case "GPT" | "gpt" | "ChatGPT" => "org.apache.kyuubi.engine.chat.provider.ChatGPTProvider"
        case "ERNIE" | "ernie" | "ErnieBot" =>
          "org.apache.kyuubi.engine.chat.provider.ErnieBotProvider"
        case other => other
      }
      .createWithDefault("ECHO")

  val ENGINE_CHAT_GPT_API_KEY: OptionalConfigEntry[String] =
    buildConf("kyuubi.engine.chat.gpt.apiKey")
      .doc("The key to access OpenAI open API, which could be got at " +
        "https://platform.openai.com/account/api-keys")
      .version("1.8.0")
      .stringConf
      .createOptional

  val ENGINE_CHAT_GPT_MODEL: ConfigEntry[String] =
    buildConf("kyuubi.engine.chat.gpt.model")
      .doc("ID of the model used in ChatGPT. Available models refer to OpenAI's " +
        "[Model overview](https://platform.openai.com/docs/models/overview).")
      .version("1.8.0")
      .stringConf
      .createWithDefault("gpt-3.5-turbo")

  val ENGINE_ERNIE_BOT_ACCESS_TOKEN: OptionalConfigEntry[String] =
    buildConf("kyuubi.engine.chat.ernie.token")
      .doc("The token to access ernie bot open API, which could be got at " +
        "https://cloud.baidu.com/doc/WENXINWORKSHOP/s/Ilkkrb0i5")
      .version("1.9.0")
      .stringConf
      .createOptional

  val ENGINE_ERNIE_BOT_MODEL: ConfigEntry[String] =
    buildConf("kyuubi.engine.chat.ernie.model")
      .doc("ID of the model used in ernie bot. " +
        "Available models are completions_pro, ernie_bot_8k, completions and eb-instant" +
        "[Model overview](https://cloud.baidu.com/doc/WENXINWORKSHOP/s/6lp69is2a).")
      .version("1.9.0")
      .stringConf
      .createWithDefault("completions")

  val ENGINE_CHAT_EXTRA_CLASSPATH: OptionalConfigEntry[String] =
    buildConf("kyuubi.engine.chat.extra.classpath")
      .doc("The extra classpath for the Chat engine, for configuring the location " +
        "of the SDK and etc.")
      .version("1.8.0")
      .stringConf
      .createOptional

  val ENGINE_CHAT_GPT_HTTP_PROXY: OptionalConfigEntry[String] =
    buildConf("kyuubi.engine.chat.gpt.http.proxy")
      .doc("HTTP proxy url for API calling in Chat GPT engine. e.g. http://127.0.0.1:1087")
      .version("1.8.0")
      .stringConf
      .createOptional

  val ENGINE_ERNIE_BOT_HTTP_PROXY: OptionalConfigEntry[String] =
    buildConf("kyuubi.engine.chat.ernie.http.proxy")
      .doc("HTTP proxy url for API calling in ernie bot engine. e.g. http://127.0.0.1:1088")
      .version("1.9.0")
      .stringConf
      .createOptional

  val ENGINE_CHAT_GPT_HTTP_CONNECT_TIMEOUT: ConfigEntry[Long] =
    buildConf("kyuubi.engine.chat.gpt.http.connect.timeout")
      .doc("The timeout[ms] for establishing the connection with the Chat GPT server. " +
        "A timeout value of zero is interpreted as an infinite timeout.")
      .version("1.8.0")
      .timeConf
      .checkValue(_ >= 0, "must be 0 or positive number")
      .createWithDefault(Duration.ofSeconds(120).toMillis)

  val ENGINE_ERNIE_HTTP_CONNECT_TIMEOUT: ConfigEntry[Long] =
    buildConf("kyuubi.engine.chat.ernie.http.connect.timeout")
      .doc("The timeout[ms] for establishing the connection with the ernie bot server. " +
        "A timeout value of zero is interpreted as an infinite timeout.")
      .version("1.9.0")
      .timeConf
      .checkValue(_ >= 0, "must be 0 or positive number")
      .createWithDefault(Duration.ofSeconds(120).toMillis)

  val ENGINE_CHAT_GPT_HTTP_SOCKET_TIMEOUT: ConfigEntry[Long] =
    buildConf("kyuubi.engine.chat.gpt.http.socket.timeout")
      .doc("The timeout[ms] for waiting for data packets after Chat GPT server " +
        "connection is established. A timeout value of zero is interpreted as an infinite timeout.")
      .version("1.8.0")
      .timeConf
      .checkValue(_ >= 0, "must be 0 or positive number")
      .createWithDefault(Duration.ofSeconds(120).toMillis)

  val ENGINE_ERNIE_HTTP_SOCKET_TIMEOUT: ConfigEntry[Long] =
    buildConf("kyuubi.engine.chat.ernie.http.socket.timeout")
      .doc("The timeout[ms] for waiting for data packets after ernie bot server " +
        "connection is established. A timeout value of zero is interpreted as an infinite timeout.")
      .version("1.9.0")
      .timeConf
      .checkValue(_ >= 0, "must be 0 or positive number")
      .createWithDefault(Duration.ofSeconds(120).toMillis)

  val ENGINE_JDBC_MEMORY: ConfigEntry[String] =
    buildConf("kyuubi.engine.jdbc.memory")
      .doc("The heap memory for the JDBC query engine")
      .version("1.6.0")
      .stringConf
      .createWithDefault("1g")

  val ENGINE_JDBC_JAVA_OPTIONS: OptionalConfigEntry[String] =
    buildConf("kyuubi.engine.jdbc.java.options")
      .doc("The extra Java options for the JDBC query engine")
      .version("1.6.0")
      .stringConf
      .createOptional

  val ENGINE_JDBC_EXTRA_CLASSPATH: OptionalConfigEntry[String] =
    buildConf("kyuubi.engine.jdbc.extra.classpath")
      .doc("The extra classpath for the JDBC query engine, for configuring the location" +
        " of the JDBC driver and etc.")
      .version("1.6.0")
      .stringConf
      .createOptional

  val ENGINE_SPARK_EVENT_LOGGERS: ConfigEntry[Seq[String]] =
    buildConf("kyuubi.engine.spark.event.loggers")
      .doc("A comma-separated list of engine loggers, where engine/session/operation etc" +
        " events go.<ul>" +
        " <li>SPARK: the events will be written to the Spark listener bus.</li>" +
        " <li>JSON: the events will be written to the location of" +
        s" ${ENGINE_EVENT_JSON_LOG_PATH.key}</li>" +
        " <li>JDBC: to be done</li>" +
        " <li>CUSTOM: to be done.</li></ul>")
      .version("1.7.0")
      .fallbackConf(ENGINE_EVENT_LOGGERS)

  val ENGINE_SPARK_PYTHON_HOME_ARCHIVE: OptionalConfigEntry[String] =
    buildConf("kyuubi.engine.spark.python.home.archive")
      .doc("Spark archive containing $SPARK_HOME/python directory, which is used to init session" +
        " Python worker for Python language mode.")
      .version("1.7.0")
      .stringConf
      .createOptional

  val ENGINE_SPARK_PYTHON_ENV_ARCHIVE: OptionalConfigEntry[String] =
    buildConf("kyuubi.engine.spark.python.env.archive")
      .doc("Portable Python env archive used for Spark engine Python language mode.")
      .version("1.7.0")
      .stringConf
      .createOptional

  val ENGINE_SPARK_PYTHON_ENV_ARCHIVE_EXEC_PATH: ConfigEntry[String] =
    buildConf("kyuubi.engine.spark.python.env.archive.exec.path")
      .doc("The Python exec path under the Python env archive.")
      .version("1.7.0")
      .stringConf
      .createWithDefault("bin/python")

  val ENGINE_SPARK_PYTHON_MAGIC_ENABLED: ConfigEntry[Boolean] =
    buildConf("kyuubi.engine.spark.python.magic.enabled")
      .internal
      .doc("Whether to enable pyspark magic node, which is helpful for notebook." +
        " See details in KYUUBI #5877")
      .version("1.9.0")
      .booleanConf
      .createWithDefault(true)

  object EngineSparkOutputMode extends Enumeration {
    type EngineSparkOutputMode = Value
    val AUTO, NOTEBOOK = Value
  }

  val ENGINE_SPARK_OUTPUT_MODE: ConfigEntry[String] =
    buildConf("kyuubi.engine.spark.output.mode")
      .doc("The output mode of Spark engine: <ul>" +
        " <li>AUTO: For PySpark, the extracted `text/plain` from python response as output.</li>" +
        " <li>NOTEBOOK: For PySpark, the original python response as output.</li></ul>")
      .version("1.9.0")
      .stringConf
      .createWithDefault(EngineSparkOutputMode.AUTO.toString)

  val ENGINE_SPARK_REGISTER_ATTRIBUTES: ConfigEntry[Seq[String]] =
    buildConf("kyuubi.engine.spark.register.attributes")
      .internal
      .doc("The extra attributes to expose when registering for Spark engine.")
      .version("1.8.0")
      .stringConf
      .toSequence()
      .createWithDefault(Seq("spark.driver.memory", "spark.executor.memory"))

  val ENGINE_SPARK_INITIALIZE_SQL: ConfigEntry[Seq[String]] =
    buildConf("kyuubi.engine.spark.initialize.sql")
      .doc("The initialize sql for Spark engine. It fallback to `kyuubi.engine.initialize.sql`.")
      .version("1.8.1")
      .fallbackConf(ENGINE_INITIALIZE_SQL)

  val ENGINE_HIVE_EVENT_LOGGERS: ConfigEntry[Seq[String]] =
    buildConf("kyuubi.engine.hive.event.loggers")
      .doc("A comma-separated list of engine history loggers, where engine/session/operation etc" +
        " events go.<ul>" +
        " <li>JSON: the events will be written to the location of" +
        s" ${ENGINE_EVENT_JSON_LOG_PATH.key}</li>" +
        " <li>JDBC: to be done</li>" +
        " <li>CUSTOM: to be done.</li></ul>")
      .version("1.7.0")
      .stringConf
      .transformToUpperCase
      .toSequence()
      .checkValue(
        _.toSet.subsetOf(Set("JSON", "JDBC", "CUSTOM")),
        "Unsupported event loggers")
      .createWithDefault(Seq("JSON"))

  val ENGINE_TRINO_EVENT_LOGGERS: ConfigEntry[Seq[String]] =
    buildConf("kyuubi.engine.trino.event.loggers")
      .doc("A comma-separated list of engine history loggers, where engine/session/operation etc" +
        " events go.<ul>" +
        " <li>JSON: the events will be written to the location of" +
        s" ${ENGINE_EVENT_JSON_LOG_PATH.key}</li>" +
        " <li>JDBC: to be done</li>" +
        " <li>CUSTOM: to be done.</li></ul>")
      .version("1.7.0")
      .stringConf
      .transformToUpperCase
      .toSequence()
      .checkValue(
        _.toSet.subsetOf(Set("JSON", "JDBC", "CUSTOM")),
        "Unsupported event loggers")
      .createWithDefault(Seq("JSON"))

  val ASYNC_EVENT_HANDLER_POLL_SIZE: ConfigEntry[Int] =
    buildConf("kyuubi.event.async.pool.size")
      .doc("Number of threads in the async event handler thread pool")
      .version("1.7.0")
      .intConf
      .createWithDefault(8)

  val ASYNC_EVENT_HANDLER_WAIT_QUEUE_SIZE: ConfigEntry[Int] =
    buildConf("kyuubi.event.async.pool.wait.queue.size")
      .doc("Size of the wait queue for the async event handler thread pool")
      .version("1.7.0")
      .intConf
      .createWithDefault(100)

  val ASYNC_EVENT_HANDLER_KEEPALIVE_TIME: ConfigEntry[Long] =
    buildConf("kyuubi.event.async.pool.keepalive.time")
      .doc("Time(ms) that an idle async thread of the async event handler thread pool will wait" +
        " for a new task to arrive before terminating")
      .version("1.7.0")
      .timeConf
      .createWithDefault(Duration.ofSeconds(60).toMillis)

  val OPERATION_GET_TABLES_IGNORE_TABLE_PROPERTIES: ConfigEntry[Boolean] =
    buildConf("kyuubi.operation.getTables.ignoreTableProperties")
      .doc("Speed up the `GetTables` operation by ignoring `tableTypes` query criteria, " +
        "and returning table identities only.")
      .version("1.8.0")
      .booleanConf
      .createWithDefault(false)

  val SERVER_LIMIT_ENGINE_CREATION: OptionalConfigEntry[Int] =
    buildConf("kyuubi.server.limit.engine.startup")
      .internal
      .doc("The maximum engine startup concurrency of kyuubi server. Highly concurrent engine" +
        " startup processes may lead to high load on the kyuubi server machine," +
        " this configuration is used to limit the number of engine startup processes" +
        " running at the same time to avoid it.")
      .version("1.8.0")
      .serverOnly
      .intConf
      .createOptional

  val KUBERNETES_FORCIBLY_REWRITE_DRIVER_POD_NAME: ConfigEntry[Boolean] =
    buildConf("kyuubi.kubernetes.spark.forciblyRewriteDriverPodName.enabled")
      .doc("Whether to forcibly rewrite Spark driver pod name with 'kyuubi-<uuid>-driver'. " +
        "If disabled, Kyuubi will try to preserve the application name while satisfying K8s' " +
        "pod name policy, but some vendors may have stricter pod name policies, thus the " +
        "generated name may become illegal.")
      .version("1.8.1")
      .booleanConf
      .createWithDefault(false)

  val KUBERNETES_FORCIBLY_REWRITE_EXEC_POD_NAME_PREFIX: ConfigEntry[Boolean] =
    buildConf("kyuubi.kubernetes.spark.forciblyRewriteExecutorPodNamePrefix.enabled")
      .doc("Whether to forcibly rewrite Spark executor pod name prefix with 'kyuubi-<uuid>'. " +
        "If disabled, Kyuubi will try to preserve the application name while satisfying K8s' " +
        "pod name policy, but some vendors may have stricter Pod name policies, thus the " +
        "generated name may become illegal.")
      .version("1.8.1")
      .booleanConf
      .createWithDefault(false)

  private val HIVE_SERVER2_THRIFT_RESULTSET_DEFAULT_FETCH_SIZE: ConfigEntry[Int] =
    buildConf("hive.server2.thrift.resultset.default.fetch.size")
      .doc("This is a hive server configuration used as a fallback conf" +
        s" for `kyuubi.server.thrift.resultset.default.fetch.size`.")
      .version("1.9.1")
      .internal
      .serverOnly
      .intConf
      .createWithDefault(1000)

  val KYUUBI_SERVER_THRIFT_RESULTSET_DEFAULT_FETCH_SIZE: ConfigEntry[Int] =
    buildConf("kyuubi.server.thrift.resultset.default.fetch.size")
      .doc("The number of rows sent in one Fetch RPC call by the server to the client, if not" +
        " specified by the client. Respect `hive.server2.thrift.resultset.default.fetch.size`" +
        " hive conf.")
      .version("1.9.1")
      .serverOnly
      .fallbackConf(HIVE_SERVER2_THRIFT_RESULTSET_DEFAULT_FETCH_SIZE)
}
