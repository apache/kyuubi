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
import org.apache.kyuubi.operation.{NoneMode, PlainStyle}
import org.apache.kyuubi.service.authentication.{AuthTypes, SaslQOP}

case class KyuubiConf(loadSysDefault: Boolean = true) extends Logging {

  private val settings = new ConcurrentHashMap[String, String]()
  private lazy val reader: ConfigProvider = new ConfigProvider(settings)
  private def loadFromMap(props: Map[String, String]): Unit = {
    settings.putAll(props.asJava)
  }

  if (loadSysDefault) {
    val fromSysDefaults = Utils.getSystemProperties.filterKeys(_.startsWith("kyuubi."))
    loadFromMap(fromSysDefaults)
  }

  def loadFileDefaults(): KyuubiConf = {
    val maybeConfigFile = Utils.getDefaultPropertiesFile()
    loadFromMap(Utils.getPropertiesFromFile(maybeConfigFile))
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
    logDeprecationWarning(key)
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

    for (e <- settings.entrySet().asScala if !e.getKey.startsWith("___")) {
      cloned.set(e.getKey, e.getValue)
    }

    for ((k, v) <- getAllWithPrefix(s"___${user}___", "")) {
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
  final val KYUUBI_BATCH_CONF_PREFIX = "kyuubi.batchConf"

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
    .doc("How often will Kyuubi server run `kinit -kt [keytab] [principal]` to renew the" +
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
      .doc("How long to wait until credentials are ready.")
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
      .doc("inactive users' credentials will be expired after a configured timeout")
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
      .doc("A comma separated list for all frontend protocols " +
        "<ul>" +
        " <li>THRIFT_BINARY - HiveServer2 compatible thrift binary protocol.</li>" +
        " <li>THRIFT_HTTP - HiveServer2 compatible thrift http protocol.</li>" +
        " <li>REST - Kyuubi defined REST API(experimental).</li> " +
        " <li>MYSQL - MySQL compatible text protocol(experimental).</li> " +
        " <li>TRINO - Trino compatible http protocol(experimental).</li> " +
        "</ul>")
      .version("1.4.0")
      .stringConf
      .toSequence()
      .transform(_.map(_.toUpperCase(Locale.ROOT)))
      .checkValue(
        _.forall(FrontendProtocols.values.map(_.toString).contains),
        s"the frontend protocol should be one or more of ${FrontendProtocols.values.mkString(",")}")
      .createWithDefault(Seq(FrontendProtocols.THRIFT_BINARY.toString))

  val FRONTEND_BIND_HOST: OptionalConfigEntry[String] = buildConf("kyuubi.frontend.bind.host")
    .doc("(deprecated) Hostname or IP of the machine on which to run the thrift frontend service " +
      "via binary protocol.")
    .version("1.0.0")
    .serverOnly
    .stringConf
    .createOptional

  val FRONTEND_THRIFT_BINARY_BIND_HOST: ConfigEntry[Option[String]] =
    buildConf("kyuubi.frontend.thrift.binary.bind.host")
      .doc("Hostname or IP of the machine on which to run the thrift frontend service " +
        "via binary protocol.")
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

  val FRONTEND_THRIFT_BINARY_SSL_DISALLOWED_PROTOCOLS: ConfigEntry[Seq[String]] =
    buildConf("kyuubi.frontend.thrift.binary.ssl.disallowed.protocols")
      .doc("SSL versions to disallow for Kyuubi thrift binary frontend.")
      .version("1.7.0")
      .stringConf
      .toSequence()
      .createWithDefault(Seq("SSLv2", "SSLv3"))

  val FRONTEND_THRIFT_BINARY_SSL_INCLUDE_CIPHER_SUITES: ConfigEntry[Seq[String]] =
    buildConf("kyuubi.frontend.thrift.binary.ssl.include.ciphersuites")
      .doc("A comma separated list of include SSL cipher suite names for thrift binary frontend.")
      .version("1.7.0")
      .stringConf
      .toSequence()
      .createWithDefault(Nil)

  @deprecated("using kyuubi.frontend.thrift.binary.bind.port instead", "1.4.0")
  val FRONTEND_BIND_PORT: ConfigEntry[Int] = buildConf("kyuubi.frontend.bind.port")
    .doc("(deprecated) Port of the machine on which to run the thrift frontend service " +
      "via binary protocol.")
    .version("1.0.0")
    .serverOnly
    .intConf
    .checkValue(p => p == 0 || (p > 1024 && p < 65535), "Invalid Port number")
    .createWithDefault(10009)

  val FRONTEND_THRIFT_BINARY_BIND_PORT: ConfigEntry[Int] =
    buildConf("kyuubi.frontend.thrift.binary.bind.port")
      .doc("Port of the machine on which to run the thrift frontend service via binary protocol.")
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
      .doc("(deprecated) Minimum number of threads in the of frontend worker thread pool for " +
        "the thrift frontend service")
      .version("1.0.0")
      .intConf
      .createWithDefault(9)

  val FRONTEND_THRIFT_MIN_WORKER_THREADS: ConfigEntry[Int] =
    buildConf("kyuubi.frontend.thrift.min.worker.threads")
      .doc("Minimum number of threads in the of frontend worker thread pool for the thrift " +
        "frontend service")
      .version("1.4.0")
      .fallbackConf(FRONTEND_MIN_WORKER_THREADS)

  val FRONTEND_MAX_WORKER_THREADS: ConfigEntry[Int] =
    buildConf("kyuubi.frontend.max.worker.threads")
      .doc("(deprecated) Maximum number of threads in the of frontend worker thread pool for " +
        "the thrift frontend service")
      .version("1.0.0")
      .intConf
      .createWithDefault(999)

  val FRONTEND_THRIFT_MAX_WORKER_THREADS: ConfigEntry[Int] =
    buildConf("kyuubi.frontend.thrift.max.worker.threads")
      .doc("Maximum number of threads in the of frontend worker thread pool for the thrift " +
        "frontend service")
      .version("1.4.0")
      .fallbackConf(FRONTEND_MAX_WORKER_THREADS)

  val FRONTEND_REST_MAX_WORKER_THREADS: ConfigEntry[Int] =
    buildConf("kyuubi.frontend.rest.max.worker.threads")
      .doc("Maximum number of threads in the of frontend worker thread pool for the rest " +
        "frontend service")
      .version("1.6.2")
      .fallbackConf(FRONTEND_MAX_WORKER_THREADS)

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

  @deprecated("using kyuubi.frontend.thrift.login.timeout instead", "1.4.0")
  val FRONTEND_LOGIN_TIMEOUT: ConfigEntry[Long] =
    buildConf("kyuubi.frontend.login.timeout")
      .doc("(deprecated) Timeout for Thrift clients during login to the thrift frontend service.")
      .version("1.0.0")
      .timeConf
      .createWithDefault(Duration.ofSeconds(20).toMillis)

  val FRONTEND_THRIFT_LOGIN_TIMEOUT: ConfigEntry[Long] =
    buildConf("kyuubi.frontend.thrift.login.timeout")
      .doc("Timeout for Thrift clients during login to the thrift frontend service.")
      .version("1.4.0")
      .fallbackConf(FRONTEND_LOGIN_TIMEOUT)

  @deprecated("using kyuubi.frontend.thrift.backoff.slot.length instead", "1.4.0")
  val FRONTEND_LOGIN_BACKOFF_SLOT_LENGTH: ConfigEntry[Long] =
    buildConf("kyuubi.frontend.backoff.slot.length")
      .doc("(deprecated) Time to back off during login to the thrift frontend service.")
      .version("1.0.0")
      .timeConf
      .createWithDefault(Duration.ofMillis(100).toMillis)

  val FRONTEND_THRIFT_LOGIN_BACKOFF_SLOT_LENGTH: ConfigEntry[Long] =
    buildConf("kyuubi.frontend.thrift.backoff.slot.length")
      .doc("Time to back off during login to the thrift frontend service.")
      .version("1.4.0")
      .fallbackConf(FRONTEND_LOGIN_BACKOFF_SLOT_LENGTH)

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

  val FRONTEND_THRIFT_HTTP_COOKIE_AUTH_ENABLED: ConfigEntry[Boolean] =
    buildConf("kyuubi.frontend.thrift.http.cookie.auth.enabled")
      .doc("When true, Kyuubi in HTTP transport mode, " +
        "will use cookie based authentication mechanism")
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
      .doc("If enabled, Kyuubi will block any requests made to it over http " +
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
      .doc("A comma separated list of exclude SSL cipher suite names for thrift http frontend.")
      .version("1.7.0")
      .stringConf
      .toSequence()
      .createWithDefault(Nil)

  val FRONTEND_THRIFT_HTTP_ALLOW_USER_SUBSTITUTION: ConfigEntry[Boolean] =
    buildConf("kyuubi.frontend.thrift.http.allow.user.substitution")
      .doc("Allow alternate user to be specified as part of open connection" +
        " request when using HTTP transport mode.")
      .version("1.6.0")
      .booleanConf
      .createWithDefault(true)

  val FRONTEND_PROXY_HTTP_CLIENT_IP_HEADER: ConfigEntry[String] =
    buildConf("kyuubi.frontend.proxy.http.client.ip.header")
      .doc("The http header to record the real client ip address. If your server is behind a load" +
        " balancer or other proxy, the server will see this load balancer or proxy IP address as" +
        " the client IP address, to get around this common issue, most load balancers or proxies" +
        " offer the ability to record the real remote IP address in an HTTP header that will be" +
        " added to the request for other devices to use. Note that, because the header value can" +
        " be specified to any ip address, so it will not be used for authentication.")
      .version("1.6.0")
      .stringConf
      .createWithDefault("X-Real-IP")

  val AUTHENTICATION_METHOD: ConfigEntry[Seq[String]] = buildConf("kyuubi.authentication")
    .doc("A comma separated list of client authentication types.<ul>" +
      " <li>NOSASL: raw transport.</li>" +
      " <li>NONE: no authentication check.</li>" +
      " <li>KERBEROS: Kerberos/GSSAPI authentication.</li>" +
      " <li>CUSTOM: User-defined authentication.</li>" +
      " <li>JDBC: JDBC query authentication.</li>" +
      " <li>LDAP: Lightweight Directory Access Protocol authentication.</li>" +
      "</ul>" +
      " Note that: For KERBEROS, it is SASL/GSSAPI mechanism," +
      " and for NONE, CUSTOM and LDAP, they are all SASL/PLAIN mechanism." +
      " If only NOSASL is specified, the authentication will be NOSASL." +
      " For SASL authentication, KERBEROS and PLAIN auth type are supported at the same time," +
      " and only the first specified PLAIN auth type is valid.")
    .version("1.0.0")
    .serverOnly
    .stringConf
    .toSequence()
    .transform(_.map(_.toUpperCase(Locale.ROOT)))
    .checkValue(
      _.forall(AuthTypes.values.map(_.toString).contains),
      s"the authentication type should be one or more of ${AuthTypes.values.mkString(",")}")
    .createWithDefault(Seq(AuthTypes.NONE.toString))

  val AUTHENTICATION_CUSTOM_CLASS: OptionalConfigEntry[String] =
    buildConf("kyuubi.authentication.custom.class")
      .doc("User-defined authentication implementation of " +
        "org.apache.kyuubi.service.authentication.PasswdAuthenticationProvider")
      .version("1.3.0")
      .stringConf
      .createOptional

  val AUTHENTICATION_LDAP_URL: OptionalConfigEntry[String] =
    buildConf("kyuubi.authentication.ldap.url")
      .doc("SPACE character separated LDAP connection URL(s).")
      .version("1.0.0")
      .stringConf
      .createOptional

  val AUTHENTICATION_LDAP_BASEDN: OptionalConfigEntry[String] =
    buildConf("kyuubi.authentication.ldap.base.dn")
      .doc("LDAP base DN.")
      .version("1.0.0")
      .stringConf
      .createOptional

  val AUTHENTICATION_LDAP_DOMAIN: OptionalConfigEntry[String] =
    buildConf("kyuubi.authentication.ldap.domain")
      .doc("LDAP domain.")
      .version("1.0.0")
      .stringConf
      .createOptional

  val AUTHENTICATION_LDAP_GUIDKEY: ConfigEntry[String] =
    buildConf("kyuubi.authentication.ldap.guidKey")
      .doc("LDAP attribute name whose values are unique in this LDAP server." +
        "For example:uid or cn.")
      .version("1.2.0")
      .stringConf
      .createWithDefault("uid")

  val AUTHENTICATION_JDBC_DRIVER: OptionalConfigEntry[String] =
    buildConf("kyuubi.authentication.jdbc.driver.class")
      .doc("Driver class name for JDBC Authentication Provider.")
      .version("1.6.0")
      .stringConf
      .createOptional

  val AUTHENTICATION_JDBC_URL: OptionalConfigEntry[String] =
    buildConf("kyuubi.authentication.jdbc.url")
      .doc("JDBC URL for JDBC Authentication Provider.")
      .version("1.6.0")
      .stringConf
      .createOptional

  val AUTHENTICATION_JDBC_USER: OptionalConfigEntry[String] =
    buildConf("kyuubi.authentication.jdbc.user")
      .doc("Database user for JDBC Authentication Provider.")
      .version("1.6.0")
      .stringConf
      .createOptional

  val AUTHENTICATION_JDBC_PASSWORD: OptionalConfigEntry[String] =
    buildConf("kyuubi.authentication.jdbc.password")
      .doc("Database password for JDBC Authentication Provider.")
      .version("1.6.0")
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
    .stringConf
    .checkValues(SaslQOP.values.map(_.toString))
    .transform(_.toLowerCase(Locale.ROOT))
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
      .doc("Maximum number of threads in the of frontend worker thread pool for the trino " +
        "frontend service")
      .version("1.7.0")
      .fallbackConf(FRONTEND_MAX_WORKER_THREADS)

  val KUBERNETES_CONTEXT: OptionalConfigEntry[String] =
    buildConf("kyuubi.kubernetes.context")
      .doc("The desired context from your kubernetes config file used to configure the K8S " +
        "client for interacting with the cluster.")
      .version("1.6.0")
      .stringConf
      .createOptional

  val KUBERNETES_NAMESPACE: ConfigEntry[String] =
    buildConf("kyuubi.kubernetes.namespace")
      .doc("The namespace that will be used for running the kyuubi pods and find engines.")
      .version("1.7.0")
      .stringConf
      .createWithDefault("default")

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
        "Note that unlike the other authentication options, this must be the exact string value " +
        "of the token to use for the authentication.")
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

  // ///////////////////////////////////////////////////////////////////////////////////////////////
  //                                 SQL Engine Configuration                                    //
  // ///////////////////////////////////////////////////////////////////////////////////////////////

  val ENGINE_ERROR_MAX_SIZE: ConfigEntry[Int] =
    buildConf("kyuubi.session.engine.startup.error.max.size")
      .doc("During engine bootstrapping, if error occurs, using this config to limit the length" +
        " error message(characters).")
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

  val SESSION_CONF_PROFILE: OptionalConfigEntry[String] =
    buildConf("kyuubi.session.conf.profile")
      .doc("Specify a profile to load session-level configurations from " +
        "`$KYUUBI_CONF_DIR/kyuubi-session-<profile>.conf`. " +
        "This configuration will be ignored if the file does not exist. " +
        "This configuration only has effect when `kyuubi.session.conf.advisor` " +
        "is set as `org.apache.kyuubi.session.FileSessionConfAdvisor`.")
      .version("1.7.0")
      .stringConf
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
      .doc("Max lifetime for spark engine, the engine will self-terminate when it reaches the" +
        " end of life. 0 or negative means not to self-terminate.")
      .version("1.6.0")
      .timeConf
      .createWithDefault(0)

  val ENGINE_FLINK_MAIN_RESOURCE: OptionalConfigEntry[String] =
    buildConf("kyuubi.session.engine.flink.main.resource")
      .doc("The package used to create Flink SQL engine remote job. If it is undefined," +
        " Kyuubi will use the default")
      .version("1.4.0")
      .stringConf
      .createOptional

  val ENGINE_FLINK_MAX_ROWS: ConfigEntry[Int] =
    buildConf("kyuubi.session.engine.flink.max.rows")
      .doc("Max rows of Flink query results. For batch queries, rows that exceeds the limit " +
        "would be ignored. For streaming queries, the query would be canceled if the limit " +
        "is reached.")
      .version("1.5.0")
      .intConf
      .createWithDefault(1000000)

  val ENGINE_TRINO_MAIN_RESOURCE: OptionalConfigEntry[String] =
    buildConf("kyuubi.session.engine.trino.main.resource")
      .doc("The package used to create Trino engine remote job. If it is undefined," +
        " Kyuubi will use the default")
      .version("1.5.0")
      .stringConf
      .createOptional

  val ENGINE_TRINO_CONNECTION_URL: OptionalConfigEntry[String] =
    buildConf("kyuubi.session.engine.trino.connection.url")
      .doc("The server url that trino engine will connect to")
      .version("1.5.0")
      .stringConf
      .createOptional

  val ENGINE_TRINO_CONNECTION_CATALOG: OptionalConfigEntry[String] =
    buildConf("kyuubi.session.engine.trino.connection.catalog")
      .doc("The default catalog that trino engine will connect to")
      .version("1.5.0")
      .stringConf
      .createOptional

  val ENGINE_TRINO_SHOW_PROGRESS: ConfigEntry[Boolean] =
    buildConf("kyuubi.session.engine.trino.showProgress")
      .doc("When true, show the progress bar and final info in the trino engine log.")
      .version("1.6.0")
      .booleanConf
      .createWithDefault(true)

  val ENGINE_TRINO_SHOW_PROGRESS_DEBUG: ConfigEntry[Boolean] =
    buildConf("kyuubi.session.engine.trino.showProgress.debug")
      .doc("When true, show the progress debug info in the trino engine log.")
      .version("1.6.0")
      .booleanConf
      .createWithDefault(false)

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

  val ENGINE_ALIVE_PROBE_ENABLED: ConfigEntry[Boolean] =
    buildConf("kyuubi.session.engine.alive.probe.enabled")
      .doc("Whether to enable the engine alive probe, it true, we will create a companion thrift" +
        " client that sends simple request to check whether the engine is keep alive.")
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
      .doc("How long to wait before retrying to open engine after a failure.")
      .version("1.7.0")
      .timeConf
      .createWithDefault(Duration.ofSeconds(10).toMillis)

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

  val SESSION_CONF_IGNORE_LIST: ConfigEntry[Seq[String]] =
    buildConf("kyuubi.session.conf.ignore.list")
      .doc("A comma separated list of ignored keys. If the client connection contains any of" +
        " them, the key and the corresponding value will be removed silently during engine" +
        " bootstrap and connection setup." +
        " Note that this rule is for server-side protection defined via administrators to" +
        " prevent some essential configs from tampering but will not forbid users to set dynamic" +
        " configurations via SET syntax.")
      .version("1.2.0")
      .stringConf
      .toSequence()
      .createWithDefault(Nil)

  val SESSION_CONF_RESTRICT_LIST: ConfigEntry[Seq[String]] =
    buildConf("kyuubi.session.conf.restrict.list")
      .doc("A comma separated list of restricted keys. If the client connection contains any of" +
        " them, the connection will be rejected explicitly during engine bootstrap and connection" +
        " setup." +
        " Note that this rule is for server-side protection defined via administrators to" +
        " prevent some essential configs from tampering but will not forbid users to set dynamic" +
        " configurations via SET syntax.")
      .version("1.2.0")
      .stringConf
      .toSequence()
      .createWithDefault(Nil)

  val SESSION_USER_SIGN_ENABLED: ConfigEntry[Boolean] =
    buildConf("kyuubi.session.user.sign.enabled")
      .doc("Whether to verify the integrity of session user name" +
        " on engine side, e.g. Authz plugin in Spark.")
      .version("1.7.0")
      .booleanConf
      .createWithDefault(false)

  val SESSION_ENGINE_STARTUP_MAX_LOG_LINES: ConfigEntry[Int] =
    buildConf("kyuubi.session.engine.startup.maxLogLines")
      .doc("The maximum number of engine log lines when errors occur during engine startup phase." +
        " Note that this max lines is for client-side to help track engine startup issue.")
      .version("1.4.0")
      .intConf
      .checkValue(_ > 0, "the maximum must be positive integer.")
      .createWithDefault(10)

  val SESSION_ENGINE_STARTUP_WAIT_COMPLETION: ConfigEntry[Boolean] =
    buildConf("kyuubi.session.engine.startup.waitCompletion")
      .doc("Whether to wait for completion after engine starts." +
        " If false, the startup process will be destroyed after the engine is started." +
        " Note that only use it when the driver is not running locally," +
        " such as yarn-cluster mode; Otherwise, the engine will be killed.")
      .version("1.5.0")
      .booleanConf
      .createWithDefault(true)

  val SESSION_ENGINE_LAUNCH_ASYNC: ConfigEntry[Boolean] =
    buildConf("kyuubi.session.engine.launch.async")
      .doc("When opening kyuubi session, whether to launch backend engine asynchronously." +
        " When true, the Kyuubi server will set up the connection with the client without delay" +
        " as the backend engine will be created asynchronously.")
      .version("1.4.0")
      .booleanConf
      .createWithDefault(true)

  val SESSION_LOCAL_DIR_ALLOW_LIST: ConfigEntry[Seq[String]] =
    buildConf("kyuubi.session.local.dir.allow.list")
      .doc("The local dir list that are allowed to access by the kyuubi session application. User" +
        " might set some parameters such as `spark.files` and it will upload some local files" +
        " when launching the kyuubi engine, if the local dir allow list is defined, kyuubi will" +
        " check whether the path to upload is in the allow list. Note that, if it is empty, there" +
        " is no limitation for that and please use absolute path list.")
      .version("1.6.0")
      .serverOnly
      .stringConf
      .checkValue(dir => dir.startsWith(File.separator), "the dir should be absolute path")
      .transform(dir => dir.stripSuffix(File.separator) + File.separator)
      .toSequence()
      .createWithDefault(Nil)

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

  val BATCH_CONF_IGNORE_LIST: ConfigEntry[Seq[String]] =
    buildConf("kyuubi.batch.conf.ignore.list")
      .doc("A comma separated list of ignored keys for batch conf. If the batch conf contains" +
        " any of them, the key and the corresponding value will be removed silently during batch" +
        " job submission." +
        " Note that this rule is for server-side protection defined via administrators to" +
        " prevent some essential configs from tampering." +
        " You can also pre-define some config for batch job submission with prefix:" +
        " kyuubi.batchConf.[batchType]. For example, you can pre-define `spark.master`" +
        " for spark batch job with key `kyuubi.batchConf.spark.spark.master`.")
      .version("1.6.0")
      .stringConf
      .toSequence()
      .createWithDefault(Nil)

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
        " metadata that is in terminate state with max age limitation.")
      .version("1.6.0")
      .booleanConf
      .createWithDefault(true)

  val METADATA_MAX_AGE: ConfigEntry[Long] =
    buildConf("kyuubi.metadata.max.age")
      .doc("The maximum age of metadata, the metadata that exceeds the age will be cleaned.")
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
      .doc("The number of threads for recovery from metadata store when Kyuubi server restarting.")
      .version("1.6.0")
      .intConf
      .createWithDefault(10)

  val METADATA_REQUEST_RETRY_THREADS: ConfigEntry[Int] =
    buildConf("kyuubi.metadata.request.retry.threads")
      .doc("Number of threads in the metadata request retry manager thread pool. The metadata" +
        " store might be unavailable sometimes and the requests will fail, to tolerant for this" +
        " case and unblock the main thread, we support to retry the failed requests in async way.")
      .version("1.6.0")
      .intConf
      .createWithDefault(10)

  val METADATA_REQUEST_RETRY_INTERVAL: ConfigEntry[Long] =
    buildConf("kyuubi.metadata.request.retry.interval")
      .doc("The interval to check and trigger the metadata request retry tasks.")
      .version("1.6.0")
      .timeConf
      .createWithDefault(Duration.ofSeconds(5).toMillis)

  val METADATA_REQUEST_RETRY_QUEUE_SIZE: ConfigEntry[Int] =
    buildConf("kyuubi.metadata.request.retry.queue.size")
      .doc("The maximum queue size for buffering metadata requests in memory when the external" +
        " metadata storage is down. Requests will be dropped if the queue exceeds.")
      .version("1.6.0")
      .intConf
      .createWithDefault(65536)

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
      .doc("Timeout for query executions at server-side, take affect with client-side timeout(" +
        "`java.sql.Statement.setQueryTimeout`) together, a running query will be cancelled" +
        " automatically if timeout. It's off by default, which means only client-side take fully" +
        " control whether the query should timeout or not. If set, client-side timeout capped at" +
        " this point. To cancel the queries right away without waiting task to finish, consider" +
        s" enabling ${OPERATION_FORCE_CANCEL.key} together.")
      .version("1.2.0")
      .timeConf
      .checkValue(_ >= 1000, "must >= 1s if set")
      .createOptional

  val OPERATION_INCREMENTAL_COLLECT: ConfigEntry[Boolean] =
    buildConf("kyuubi.operation.incremental.collect")
      .internal
      .doc("When true, the executor side result will be sequentially calculated and returned to" +
        " the Spark driver side.")
      .version("1.4.0")
      .booleanConf
      .createWithDefault(false)

  val OPERATION_RESULT_CODEC: ConfigEntry[String] =
    buildConf("kyuubi.operation.result.codec")
      .doc("Specify the result codec, available configs are: <ul>" +
        " <li>SIMPLE: the result will convert to TRow at the engine driver side. </li>" +
        " <li>ARROW: the result will be encoded as Arrow at the executor side before collecting" +
        " by the driver, and deserialized at the client side. note that it only takes effect for" +
        " kyuubi-hive-jdbc clients now.</li></ul>")
      .version("1.7.0")
      .stringConf
      .checkValues(Set("arrow", "simple"))
      .transform(_.toLowerCase(Locale.ROOT))
      .createWithDefault("simple")

  val OPERATION_RESULT_MAX_ROWS: ConfigEntry[Int] =
    buildConf("kyuubi.operation.result.max.rows")
      .doc("Max rows of Spark query results. Rows that exceeds the limit would be ignored. " +
        "By setting this value to 0 to disable the max rows limit.")
      .version("1.6.0")
      .intConf
      .createWithDefault(0)

  val SERVER_OPERATION_LOG_DIR_ROOT: ConfigEntry[String] =
    buildConf("kyuubi.operation.log.dir.root")
      .doc("Root directory for query operation log at server-side.")
      .version("1.4.0")
      .serverOnly
      .stringConf
      .createWithDefault("server_operation_logs")

  @deprecated("using kyuubi.engine.share.level instead", "1.2.0")
  val LEGACY_ENGINE_SHARE_LEVEL: ConfigEntry[String] =
    buildConf("kyuubi.session.engine.share.level")
      .doc(s"(deprecated) - Using kyuubi.engine.share.level instead")
      .version("1.0.0")
      .stringConf
      .transform(_.toUpperCase(Locale.ROOT))
      .checkValues(ShareLevel.values.map(_.toString))
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
      .transform(_.toLowerCase(Locale.ROOT))
      .checkValue(validZookeeperSubPath.matcher(_).matches(), "must be valid zookeeper sub path.")
      .createOptional

  val ENGINE_SHARE_LEVEL_SUBDOMAIN: ConfigEntry[Option[String]] =
    buildConf("kyuubi.engine.share.level.subdomain")
      .doc("Allow end-users to create a subdomain for the share level of an engine. A" +
        " subdomain is a case-insensitive string values that must be a valid zookeeper sub path." +
        " For example, for `USER` share level, an end-user can share a certain engine within" +
        " a subdomain, not for all of its clients. End-users are free to create multiple" +
        " engines in the `USER` share level. When disable engine pool, use 'default' if absent.")
      .version("1.4.0")
      .fallbackConf(ENGINE_SHARE_LEVEL_SUB_DOMAIN)

  @deprecated("using kyuubi.frontend.connection.url.use.hostname instead, 1.5.0")
  val ENGINE_CONNECTION_URL_USE_HOSTNAME: ConfigEntry[Boolean] =
    buildConf("kyuubi.engine.connection.url.use.hostname")
      .doc("(deprecated) " +
        "When true, engine register with hostname to zookeeper. When spark run on k8s" +
        " with cluster mode, set to false to ensure that server can connect to engine")
      .version("1.3.0")
      .booleanConf
      .createWithDefault(true)

  val FRONTEND_CONNECTION_URL_USE_HOSTNAME: ConfigEntry[Boolean] =
    buildConf("kyuubi.frontend.connection.url.use.hostname")
      .doc("When true, frontend services prefer hostname, otherwise, ip address. Note that, " +
        "the default value is set to `false` when engine running on Kubernetes to prevent " +
        "potential network issue.")
      .version("1.5.0")
      .fallbackConf(ENGINE_CONNECTION_URL_USE_HOSTNAME)

  val ENGINE_SHARE_LEVEL: ConfigEntry[String] = buildConf("kyuubi.engine.share.level")
    .doc("Engines will be shared in different levels, available configs are: <ul>" +
      " <li>CONNECTION: engine will not be shared but only used by the current client" +
      " connection</li>" +
      " <li>USER: engine will be shared by all sessions created by a unique username," +
      s" see also ${ENGINE_SHARE_LEVEL_SUBDOMAIN.key}</li>" +
      " <li>GROUP: engine will be shared by all sessions created by all users belong to the same" +
      " primary group name. The engine will be launched by the group name as the effective" +
      " username, so here the group name is kind of special user who is able to visit the" +
      " compute resources/data of a team. It follows the" +
      " [Hadoop GroupsMapping](https://reurl.cc/xE61Y5) to map user to a primary group. If the" +
      " primary group is not found, it fallback to the USER level." +
      " <li>SERVER: the App will be shared by Kyuubi servers</li></ul>")
    .version("1.2.0")
    .fallbackConf(LEGACY_ENGINE_SHARE_LEVEL)

  val ENGINE_TYPE: ConfigEntry[String] = buildConf("kyuubi.engine.type")
    .doc("Specify the detailed engine that supported by the Kyuubi. The engine type bindings to" +
      " SESSION scope. This configuration is experimental. Currently, available configs are: <ul>" +
      " <li>SPARK_SQL: specify this engine type will launch a Spark engine which can provide" +
      " all the capacity of the Apache Spark. Note, it's a default engine type.</li>" +
      " <li>FLINK_SQL: specify this engine type will launch a Flink engine which can provide" +
      " all the capacity of the Apache Flink.</li>" +
      " <li>TRINO: specify this engine type will launch a Trino engine which can provide" +
      " all the capacity of the Trino.</li>" +
      " <li>HIVE_SQL: specify this engine type will launch a Hive engine which can provide" +
      " all the capacity of the Hive Server2.</li>" +
      " <li>JDBC: specify this engine type will launch a JDBC engine which can provide" +
      " a mysql protocol connector, for now we only support Doris dialect.</li>" +
      "</ul>")
    .version("1.4.0")
    .stringConf
    .transform(_.toUpperCase(Locale.ROOT))
    .checkValues(EngineType.values.map(_.toString))
    .createWithDefault(EngineType.SPARK_SQL.toString)

  val ENGINE_POOL_NAME: ConfigEntry[String] = buildConf("kyuubi.engine.pool.name")
    .doc("The name of engine pool.")
    .version("1.5.0")
    .stringConf
    .checkValue(validZookeeperSubPath.matcher(_).matches(), "must be valid zookeeper sub path.")
    .createWithDefault("engine-pool")

  val ENGINE_POOL_SIZE_THRESHOLD: ConfigEntry[Int] = buildConf("kyuubi.engine.pool.size.threshold")
    .doc("This parameter is introduced as a server-side parameter, " +
      "and controls the upper limit of the engine pool.")
    .version("1.4.0")
    .intConf
    .checkValue(s => s > 0 && s < 33, "Invalid engine pool threshold, it should be in [1, 32]")
    .createWithDefault(9)

  val ENGINE_POOL_SIZE: ConfigEntry[Int] = buildConf("kyuubi.engine.pool.size")
    .doc("The size of engine pool. Note that, " +
      "if the size is less than 1, the engine pool will not be enabled; " +
      "otherwise, the size of the engine pool will be " +
      s"min(this, ${ENGINE_POOL_SIZE_THRESHOLD.key}).")
    .version("1.4.0")
    .intConf
    .createWithDefault(-1)

  val ENGINE_POOL_BALANCE_POLICY: ConfigEntry[String] =
    buildConf("kyuubi.engine.pool.selectPolicy")
      .doc("The select policy of an engine from the corresponding engine pool engine for " +
        "a session. <ul>" +
        "<li>RANDOM - Randomly use the engine in the pool</li>" +
        "<li>POLLING - Polling use the engine in the pool</li>" +
        "</ul>")
      .version("1.7.0")
      .stringConf
      .transform(_.toUpperCase(Locale.ROOT))
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

  val ENGINE_DEREGISTER_EXCEPTION_CLASSES: ConfigEntry[Seq[String]] =
    buildConf("kyuubi.engine.deregister.exception.classes")
      .doc("A comma separated list of exception classes. If there is any exception thrown," +
        " whose class matches the specified classes, the engine would deregister itself.")
      .version("1.2.0")
      .stringConf
      .toSequence()
      .createWithDefault(Nil)

  val ENGINE_DEREGISTER_EXCEPTION_MESSAGES: ConfigEntry[Seq[String]] =
    buildConf("kyuubi.engine.deregister.exception.messages")
      .doc("A comma separated list of exception messages. If there is any exception thrown," +
        " whose message or stacktrace matches the specified message list, the engine would" +
        " deregister itself.")
      .version("1.2.0")
      .stringConf
      .toSequence()
      .createWithDefault(Nil)

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
      .doc("The scheduler pool of job. Note that, this config should be used after change Spark " +
        "config spark.scheduler.mode=FAIR.")
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
        "all the JDBC/ODBC connections will be isolated against the user. Including: " +
        "the temporary views, function registries, SQL configuration and the current database. " +
        "Note that, it does not affect if the share level is connection or user.")
      .version("1.6.0")
      .booleanConf
      .createWithDefault(true)

  val ENGINE_USER_ISOLATED_SPARK_SESSION_IDLE_TIMEOUT: ConfigEntry[Long] =
    buildConf("kyuubi.engine.user.isolated.spark.session.idle.timeout")
      .doc(s"If ${ENGINE_USER_ISOLATED_SPARK_SESSION.key} is false, we will release the " +
        s"spark session if its corresponding user is inactive after this configured timeout.")
      .version("1.6.0")
      .timeConf
      .createWithDefault(Duration.ofHours(6).toMillis)

  val ENGINE_USER_ISOLATED_SPARK_SESSION_IDLE_INTERVAL: ConfigEntry[Long] =
    buildConf("kyuubi.engine.user.isolated.spark.session.idle.interval")
      .doc(s"The interval to check if the user isolated spark session is timeout.")
      .version("1.6.0")
      .timeConf
      .createWithDefault(Duration.ofMinutes(1).toMillis)

  val SERVER_EVENT_JSON_LOG_PATH: ConfigEntry[String] =
    buildConf("kyuubi.backend.server.event.json.log.path")
      .doc("The location of server events go for the builtin JSON logger")
      .version("1.4.0")
      .serverOnly
      .stringConf
      .createWithDefault("file:///tmp/kyuubi/events")

  val ENGINE_EVENT_JSON_LOG_PATH: ConfigEntry[String] =
    buildConf("kyuubi.engine.event.json.log.path")
      .doc("The location of all the engine events go for the builtin JSON logger.<ul>" +
        "<li>Local Path: start with 'file://'</li>" +
        "<li>HDFS Path: start with 'hdfs://'</li></ul>")
      .version("1.3.0")
      .stringConf
      .createWithDefault("file:///tmp/kyuubi/events")

  val SERVER_EVENT_LOGGERS: ConfigEntry[Seq[String]] =
    buildConf("kyuubi.backend.server.event.loggers")
      .doc("A comma separated list of server history loggers, where session/operation etc" +
        " events go.<ul>" +
        s" <li>JSON: the events will be written to the location of" +
        s" ${SERVER_EVENT_JSON_LOG_PATH.key}</li>" +
        s" <li>JDBC: to be done</li>" +
        s" <li>CUSTOM: to be done.</li></ul>")
      .version("1.4.0")
      .serverOnly
      .stringConf
      .transform(_.toUpperCase(Locale.ROOT))
      .toSequence()
      .checkValue(_.toSet.subsetOf(Set("JSON", "JDBC", "CUSTOM")), "Unsupported event loggers")
      .createWithDefault(Nil)

  @deprecated("using kyuubi.engine.spark.event.loggers instead", "1.6.0")
  val ENGINE_EVENT_LOGGERS: ConfigEntry[Seq[String]] =
    buildConf("kyuubi.engine.event.loggers")
      .doc("A comma separated list of engine history loggers, where engine/session/operation etc" +
        " events go.<ul>" +
        " <li>SPARK: the events will be written to the spark listener bus.</li>" +
        " <li>JSON: the events will be written to the location of" +
        s" ${ENGINE_EVENT_JSON_LOG_PATH.key}</li>" +
        " <li>JDBC: to be done</li>" +
        " <li>CUSTOM: to be done.</li></ul>")
      .version("1.3.0")
      .stringConf
      .transform(_.toUpperCase(Locale.ROOT))
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
        "subclass of EngineSecuritySecretProvider.")
      .version("1.5.0")
      .stringConf
      .createWithDefault(
        "org.apache.kyuubi.service.authentication.ZooKeeperEngineSecuritySecretProviderImpl")

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
      .doc("A human readable name of session and we use empty string by default. " +
        "This name will be recorded in event. Note that, we only apply this value from " +
        "session conf.")
      .version("1.4.0")
      .stringConf
      .createOptional

  val OPERATION_PLAN_ONLY_MODE: ConfigEntry[String] =
    buildConf("kyuubi.operation.plan.only.mode")
      .doc("Configures the statement performed mode, The value can be 'parse', 'analyze', " +
        "'optimize', 'optimize_with_stats', 'physical', 'execution', or 'none', " +
        "when it is 'none', indicate to the statement will be fully executed, otherwise " +
        "only way without executing the query. different engines currently support different " +
        "modes, the Spark engine supports all modes, and the Flink engine supports 'parse', " +
        "'physical', and 'execution', other engines do not support planOnly currently.")
      .version("1.4.0")
      .stringConf
      .transform(_.toUpperCase(Locale.ROOT))
      .checkValue(
        mode =>
          Set(
            "PARSE",
            "ANALYZE",
            "OPTIMIZE",
            "OPTIMIZE_WITH_STATS",
            "PHYSICAL",
            "EXECUTION",
            "NONE").contains(mode),
        "Invalid value for 'kyuubi.operation.plan.only.mode'. Valid values are" +
          "'parse', 'analyze', 'optimize', 'optimize_with_stats', 'physical', 'execution' and " +
          "'none'.")
      .createWithDefault(NoneMode.name)

  val OPERATION_PLAN_ONLY_OUT_STYLE: ConfigEntry[String] =
    buildConf("kyuubi.operation.plan.only.output.style")
      .doc("Configures the planOnly output style, The value can be 'plain' and 'json', default " +
        "value is 'plain', this configuration supports only the output styles of the Spark engine")
      .version("1.7.0")
      .stringConf
      .transform(_.toUpperCase(Locale.ROOT))
      .checkValue(
        mode => Set("PLAIN", "JSON").contains(mode),
        "Invalid value for 'kyuubi.operation.plan.only.output.style'. Valid values are " +
          "'plain', 'json'.")
      .createWithDefault(PlainStyle.name)

  val OPERATION_PLAN_ONLY_EXCLUDES: ConfigEntry[Seq[String]] =
    buildConf("kyuubi.operation.plan.only.excludes")
      .doc("Comma-separated list of query plan names, in the form of simple class names, i.e, " +
        "for `set abc=xyz`, the value will be `SetCommand`. For those auxiliary plans, such as " +
        "`switch databases`, `set properties`, or `create temporary view` e.t.c, " +
        "which are used for setup evaluating environments for analyzing actual queries, " +
        "we can use this config to exclude them and let them take effect. " +
        s"See also ${OPERATION_PLAN_ONLY_MODE.key}.")
      .version("1.5.0")
      .stringConf
      .toSequence()
      .createWithDefault(Seq(
        "ResetCommand",
        "SetCommand",
        "SetNamespaceCommand",
        "UseStatement",
        "SetCatalogAndNamespace"))

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
        " <ul><li>SQL: (Default) Run all following statements as SQL queries.</li>" +
        " <li>SCALA: Run all following input a scala codes</li></ul>")
      .version("1.5.0")
      .stringConf
      .transform(_.toUpperCase(Locale.ROOT))
      .checkValues(OperationLanguages.values.map(_.toString))
      .createWithDefault(OperationLanguages.SQL.toString)

  val SESSION_CONF_ADVISOR: OptionalConfigEntry[String] =
    buildConf("kyuubi.session.conf.advisor")
      .doc("A config advisor plugin for Kyuubi Server. This plugin can provide some custom " +
        "configs for different user or session configs and overwrite the session configs before " +
        "open a new session. This config value should be a class which is a child of " +
        "'org.apache.kyuubi.plugin.SessionConfAdvisor' which has zero-arg constructor.")
      .version("1.5.0")
      .stringConf
      .createOptional

  val GROUP_PROVIDER: ConfigEntry[String] =
    buildConf("kyuubi.session.group.provider")
      .doc("A group provider plugin for Kyuubi Server. This plugin can provide primary group " +
        "and groups information for different user or session configs. This config value " +
        "should be a class which is a child of 'org.apache.kyuubi.plugin.GroupProvider' which " +
        "has zero-arg constructor. Kyuubi provides the following built-in implementations: " +
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
      .doc("When true, show the progress bar in the spark engine log.")
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

  val ENGINE_TRINO_MEMORY: ConfigEntry[String] =
    buildConf("kyuubi.engine.trino.memory")
      .doc("The heap memory for the trino query engine")
      .version("1.6.0")
      .stringConf
      .createWithDefault("1g")

  val ENGINE_TRINO_JAVA_OPTIONS: OptionalConfigEntry[String] =
    buildConf("kyuubi.engine.trino.java.options")
      .doc("The extra java options for the trino query engine")
      .version("1.6.0")
      .stringConf
      .createOptional

  val ENGINE_TRINO_EXTRA_CLASSPATH: OptionalConfigEntry[String] =
    buildConf("kyuubi.engine.trino.extra.classpath")
      .doc("The extra classpath for the trino query engine, " +
        "for configuring other libs which may need by the trino engine ")
      .version("1.6.0")
      .stringConf
      .createOptional

  val ENGINE_HIVE_MEMORY: ConfigEntry[String] =
    buildConf("kyuubi.engine.hive.memory")
      .doc("The heap memory for the hive query engine")
      .version("1.6.0")
      .stringConf
      .createWithDefault("1g")

  val ENGINE_HIVE_JAVA_OPTIONS: OptionalConfigEntry[String] =
    buildConf("kyuubi.engine.hive.java.options")
      .doc("The extra java options for the hive query engine")
      .version("1.6.0")
      .stringConf
      .createOptional

  val ENGINE_HIVE_EXTRA_CLASSPATH: OptionalConfigEntry[String] =
    buildConf("kyuubi.engine.hive.extra.classpath")
      .doc("The extra classpath for the hive query engine, for configuring location" +
        " of hadoop client jars, etc")
      .version("1.6.0")
      .stringConf
      .createOptional

  val ENGINE_FLINK_MEMORY: ConfigEntry[String] =
    buildConf("kyuubi.engine.flink.memory")
      .doc("The heap memory for the flink sql engine")
      .version("1.6.0")
      .stringConf
      .createWithDefault("1g")

  val ENGINE_FLINK_JAVA_OPTIONS: OptionalConfigEntry[String] =
    buildConf("kyuubi.engine.flink.java.options")
      .doc("The extra java options for the flink sql engine")
      .version("1.6.0")
      .stringConf
      .createOptional

  val ENGINE_FLINK_EXTRA_CLASSPATH: OptionalConfigEntry[String] =
    buildConf("kyuubi.engine.flink.extra.classpath")
      .doc("The extra classpath for the flink sql engine, for configuring location" +
        " of hadoop client jars, etc")
      .version("1.6.0")
      .stringConf
      .createOptional

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

  val SERVER_LIMIT_CONNECTIONS_USER_UNLIMITED_LIST: ConfigEntry[Seq[String]] =
    buildConf("kyuubi.server.limit.connections.user.unlimited.list")
      .doc("The maximin connections of the user in the white list will not be limited.")
      .version("1.7.0")
      .serverOnly
      .stringConf
      .toSequence()
      .createWithDefault(Nil)

  val SERVER_LIMIT_BATCH_CONNECTIONS_PER_USER: OptionalConfigEntry[Int] =
    buildConf("kyuubi.server.batch.limit.connections.per.user")
      .doc("Maximum kyuubi server batch connections per user." +
        " Any user exceeding this limit will not be allowed to connect.")
      .version("1.7.0")
      .serverOnly
      .intConf
      .createOptional

  val SERVER_LIMIT_BATCH_CONNECTIONS_PER_IPADDRESS: OptionalConfigEntry[Int] =
    buildConf("kyuubi.server.batch.limit.connections.per.ipaddress")
      .doc("Maximum kyuubi server batch connections per ipaddress." +
        " Any user exceeding this limit will not be allowed to connect.")
      .version("1.7.0")
      .serverOnly
      .intConf
      .createOptional

  val SERVER_LIMIT_BATCH_CONNECTIONS_PER_USER_IPADDRESS: OptionalConfigEntry[Int] =
    buildConf("kyuubi.server.batch.limit.connections.per.user.ipaddress")
      .doc("Maximum kyuubi server batch connections per user:ipaddress combination." +
        " Any user-ipaddress exceeding this limit will not be allowed to connect.")
      .version("1.7.0")
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

  val OPERATION_SPARK_LISTENER_ENABLED: ConfigEntry[Boolean] =
    buildConf("kyuubi.operation.spark.listener.enabled")
      .doc("When set to true, Spark engine registers a SQLOperationListener before executing " +
        "the statement, logs a few summary statistics when each stage completes.")
      .version("1.6.0")
      .booleanConf
      .createWithDefault(true)

  val ENGINE_JDBC_DRIVER_CLASS: OptionalConfigEntry[String] =
    buildConf("kyuubi.engine.jdbc.driver.class")
      .doc("The driver class for jdbc engine connection")
      .version("1.6.0")
      .stringConf
      .createOptional

  val ENGINE_JDBC_CONNECTION_URL: OptionalConfigEntry[String] =
    buildConf("kyuubi.engine.jdbc.connection.url")
      .doc("The server url that engine will connect to")
      .version("1.6.0")
      .stringConf
      .createOptional

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
      .doc("The connection provider is used for getting a connection from server")
      .version("1.6.0")
      .stringConf
      .createOptional

  val ENGINE_JDBC_SHORT_NAME: OptionalConfigEntry[String] =
    buildConf("kyuubi.engine.jdbc.type")
      .doc("The short name of jdbc type")
      .version("1.6.0")
      .stringConf
      .createOptional

  val ENGINE_OPERATION_CONVERT_CATALOG_DATABASE_ENABLED: ConfigEntry[Boolean] =
    buildConf("kyuubi.engine.operation.convert.catalog.database.enabled")
      .doc("When set to true, The engine converts the JDBC methods of set/get Catalog " +
        "and set/get Schema to the implementation of different engines")
      .version("1.6.0")
      .booleanConf
      .createWithDefault(true)

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
        FRONTEND_LOGIN_TIMEOUT.key,
        "1.4.0",
        s"Use ${FRONTEND_THRIFT_LOGIN_TIMEOUT.key} instead"),
      DeprecatedConfig(
        FRONTEND_LOGIN_BACKOFF_SLOT_LENGTH.key,
        "1.4.0",
        s"Use ${FRONTEND_THRIFT_LOGIN_BACKOFF_SLOT_LENGTH.key} instead"),
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

      // deprected configs of [[org.apache.kyuubi.zookeeper.ZookeeperConf]]
      DeprecatedConfig(
        "kyuubi.zookeeper.embedded.port",
        "1.2.0",
        "Use kyuubi.zookeeper.embedded.client.port instead"),
      DeprecatedConfig(
        "kyuubi.zookeeper.embedded.directory",
        "1.2.0",
        "Use kyuubi.zookeeper.embedded.data.dir instead"),

      // deprected configs of [[org.apache.kyuubi.ha.HighAvailabilityConf]]
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

  val ENGINE_JDBC_MEMORY: ConfigEntry[String] =
    buildConf("kyuubi.engine.jdbc.memory")
      .doc("The heap memory for the jdbc query engine")
      .version("1.6.0")
      .stringConf
      .createWithDefault("1g")

  val ENGINE_JDBC_JAVA_OPTIONS: OptionalConfigEntry[String] =
    buildConf("kyuubi.engine.jdbc.java.options")
      .doc("The extra java options for the jdbc query engine")
      .version("1.6.0")
      .stringConf
      .createOptional

  val ENGINE_JDBC_EXTRA_CLASSPATH: OptionalConfigEntry[String] =
    buildConf("kyuubi.engine.jdbc.extra.classpath")
      .doc("The extra classpath for the jdbc query engine, for configuring location" +
        " of jdbc driver, etc")
      .version("1.6.0")
      .stringConf
      .createOptional

  val ENGINE_SPARK_EVENT_LOGGERS: ConfigEntry[Seq[String]] =
    buildConf("kyuubi.engine.spark.event.loggers")
      .doc("A comma separated list of engine loggers, where engine/session/operation etc" +
        " events go.<ul>" +
        " <li>SPARK: the events will be written to the spark listener bus.</li>" +
        " <li>JSON: the events will be written to the location of" +
        s" ${ENGINE_EVENT_JSON_LOG_PATH.key}</li>" +
        " <li>JDBC: to be done</li>" +
        " <li>CUSTOM: to be done.</li></ul>")
      .version("1.7.0")
      .fallbackConf(ENGINE_EVENT_LOGGERS)

  val ENGINE_SPARK_PYTHON_HOME_ARCHIVE: OptionalConfigEntry[String] =
    buildConf("kyuubi.engine.spark.python.home.archive")
      .doc("Spark archive containing $SPARK_HOME/python directory, which is used to init session" +
        " python worker for python language mode.")
      .version("1.7.0")
      .stringConf
      .createOptional

  val ENGINE_SPARK_PYTHON_ENV_ARCHIVE: OptionalConfigEntry[String] =
    buildConf("kyuubi.engine.spark.python.env.archive")
      .doc("Portable python env archive used for Spark engine python language mode.")
      .version("1.7.0")
      .stringConf
      .createOptional

  val ENGINE_SPARK_PYTHON_ENV_ARCHIVE_EXEC_PATH: ConfigEntry[String] =
    buildConf("kyuubi.engine.spark.python.env.archive.exec.path")
      .doc("The python exec path under the python env archive.")
      .version("1.7.0")
      .stringConf
      .createWithDefault("bin/python")

  val ENGINE_HIVE_EVENT_LOGGERS: ConfigEntry[Seq[String]] =
    buildConf("kyuubi.engine.hive.event.loggers")
      .doc("A comma separated list of engine history loggers, where engine/session/operation etc" +
        " events go.<ul>" +
        " <li>JSON: the events will be written to the location of" +
        s" ${ENGINE_EVENT_JSON_LOG_PATH.key}</li>" +
        " <li>JDBC: to be done</li>" +
        " <li>CUSTOM: to be done.</li></ul>")
      .version("1.7.0")
      .stringConf
      .transform(_.toUpperCase(Locale.ROOT))
      .toSequence()
      .checkValue(
        _.toSet.subsetOf(Set("JSON", "JDBC", "CUSTOM")),
        "Unsupported event loggers")
      .createWithDefault(Seq("JSON"))

  val ENGINE_TRINO_EVENT_LOGGERS: ConfigEntry[Seq[String]] =
    buildConf("kyuubi.engine.trino.event.loggers")
      .doc("A comma separated list of engine history loggers, where engine/session/operation etc" +
        " events go.<ul>" +
        " <li>JSON: the events will be written to the location of" +
        s" ${ENGINE_EVENT_JSON_LOG_PATH.key}</li>" +
        " <li>JDBC: to be done</li>" +
        " <li>CUSTOM: to be done.</li></ul>")
      .version("1.7.0")
      .stringConf
      .transform(_.toUpperCase(Locale.ROOT))
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
}
