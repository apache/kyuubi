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

package org.apache.spark

import java.io.File
import java.lang.reflect.{InvocationTargetException, UndeclaredThrowableException}
import java.net.URI

import scala.annotation.tailrec
import scala.collection.Map
import scala.util.Try
import scala.util.matching.Regex

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.security.UserGroupInformation
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.util._
import org.slf4j.Logger

import yaooqinn.kyuubi.Logging

/**
 * Wrapper for [[Utils]] and [[SparkHadoopUtil]]
 */
object KyuubiSparkUtil extends Logging {
  // PREFIXES
  val SPARK_PREFIX = "spark."
  private val YARN_PREFIX = "yarn."
  private val HADOOP_PRFIX = "hadoop."
  val SPARK_HADOOP_PREFIX: String = SPARK_PREFIX + HADOOP_PRFIX
  private val SPARK_YARN_PREFIX: String = SPARK_PREFIX + YARN_PREFIX
  private val DRIVER_PREFIX = "driver."
  private val AM_PREFIX = SPARK_PREFIX + YARN_PREFIX + "am."

  private val UI_PREFIX = "ui."
  private val SQL_PREFIX = "sql."
  private val HIVE_PREFIX = "hive."
  private val METASTORE_PREFIX = "metastore."

  // ENVIRONMENTS
  val SPARK_HOME: String = System.getenv("SPARK_HOME")
  val SPARK_JARS_DIR: String = SPARK_HOME + File.separator + "jars"

  // YARN
  val KEYTAB: String = SPARK_YARN_PREFIX + "keytab"
  val PRINCIPAL: String = SPARK_YARN_PREFIX + "principal"
  val MAX_APP_ATTEMPTS: String = SPARK_YARN_PREFIX + "maxAppAttempts"
  val SPARK_YARN_JARS: String = SPARK_YARN_PREFIX + "jars"
  val ACCESS_NNS: String = SPARK_YARN_PREFIX + "access.namenodes"
  val ACCESS_FSS: String = SPARK_YARN_PREFIX + "access.hadoopFileSystems"
  val STAGING_DIR: String = SPARK_YARN_PREFIX + "stagingDir"

  // DRIVER
  val DRIVER_BIND_ADDR: String = SPARK_PREFIX + DRIVER_PREFIX + "bindAddress"
  val DRIVER_MEM: String = SPARK_PREFIX + DRIVER_PREFIX + "memory"
  val DRIVER_MEM_OVERHEAD: String = SPARK_PREFIX + YARN_PREFIX + DRIVER_PREFIX + "memoryOverhead"
  val DRIVER_CORES: String = SPARK_PREFIX + DRIVER_PREFIX + "cores"
  val DRIVER_EXTRA_JAVA_OPTIONS: String = SPARK_PREFIX + DRIVER_PREFIX + "extraJavaOptions"

  val GC_INTERVAL: String = SPARK_PREFIX + "cleaner.periodicGC.interval"
  val GC_INTERVAL_DEFAULT: String = "3min"

  val AM_EXTRA_JAVA_OPTIONS: String = AM_PREFIX + "extraJavaOptions"

  val SPARK_UI_PORT: String = SPARK_PREFIX + UI_PREFIX + "port"
  val SPARK_UI_PORT_DEFAULT = "0"

  val MULTIPLE_CONTEXTS: String = SPARK_PREFIX + DRIVER_PREFIX + "allowMultipleContexts"
  val MULTIPLE_CONTEXTS_DEFAULT = "true"

  // Spark SQL
  val CATALOG_IMPL: String = SPARK_PREFIX + SQL_PREFIX + "catalogImplementation"
  val CATALOG_IMPL_DEFAULT = "hive"
  val CONVERT_METASTORE_ORC: String =
    SPARK_PREFIX + SQL_PREFIX + HIVE_PREFIX + "convertMetastoreOrc"
  val ORC_IMPL: String = SPARK_PREFIX + SQL_PREFIX + "orc.impl"
  val ORC_IMPL_DEFAULT = "native"
  val ORC_VECTORIZED_READER_ENABLED: String =
    SPARK_PREFIX + SQL_PREFIX + "orc.enableVectorizedReader"

  val DEPLOY_MODE: String = SPARK_PREFIX + "submit.deployMode"
  val DEPLOY_MODE_DEFAULT = "client"

  // Hive
  val METASTORE_JARS: String = SPARK_PREFIX + SQL_PREFIX + HIVE_PREFIX + METASTORE_PREFIX + "jars"
  val METASTORE_KEYTAB: String =
    SPARK_HADOOP_PREFIX + HIVE_PREFIX + METASTORE_PREFIX + "kerberos.keytab.file"
  val METASTORE_PRINCIPAL: String =
    SPARK_HADOOP_PREFIX + HIVE_PREFIX + METASTORE_PREFIX + "kerberos.principal"

  val SPARK_LOCAL_DIR: String = SPARK_PREFIX + "local.dir"

  val HIVE_VAR_PREFIX: Regex = """set:hivevar:([^=]+)""".r
  val USE_DB = "use:database"
  val QUEUE: String = SPARK_PREFIX + YARN_PREFIX + "queue"
  val DEPRECATED_QUEUE = "mapred.job.queue.name"
  val HDFS_CLIENT_CACHE: String = SPARK_HADOOP_PREFIX + "fs.hdfs.impl.disable.cache"
  val HDFS_CLIENT_CACHE_DEFAULT = "true"
  val FILE_CLIENT_CACHE: String = SPARK_HADOOP_PREFIX + "fs.file.impl.disable.cache"
  val FILE_CLIENT_CACHE_DEFAULT = "true"
  val RDD_PAR_LISTING: String = SPARK_PREFIX + "rdd.parallelListingThreshold"

  // Runtime Spark Version
  val SPARK_VERSION: String = org.apache.spark.SPARK_VERSION

  def addShutdownHook(f: => Unit): Unit = {
    ShutdownHookManager.addShutdownHook(() => f)
  }

  def initDaemon(log: Logger): Unit = {
    Utils.initDaemon(log)
  }

  def getJobGroupIDKey: String = SparkContext.SPARK_JOB_GROUP_ID

  def exceptionString(e: Throwable): String = {
    Utils.exceptionString(e)
  }

  def getCurrentUserName: String = {
    UserGroupInformation.getCurrentUser.getShortUserName
  }

  def getContextOrSparkClassLoader: ClassLoader = {
    Utils.getContextOrSparkClassLoader
  }

  def createTempDir(
      root: String = System.getProperty("java.io.tmpdir"),
      namePrefix: String = "spark"): File = {
    Utils.createTempDir(root, namePrefix)
  }

  def newConfiguration(conf: SparkConf): Configuration = {
    SparkHadoopUtil.get.newConfiguration(conf)
  }

  def localHostName(): String = Utils.localHostName()

  // org.apache.spark.util.VersionUtils: Utilities for working with Spark version strings

  def majorVersion(sparkVersion: String): Int = VersionUtils.majorVersion(sparkVersion)

  def minorVersion(sparkVersion: String): Int = VersionUtils.minorVersion(sparkVersion)

  /**
   * Check whether the runtime version of Spark is >= the specified one.
   */
  def equalOrHigherThan(version: String): Boolean = {
    val tMajor = majorVersion(SPARK_VERSION)
    val tMinor = minorVersion(SPARK_VERSION)
    val sMajor = majorVersion(version)
    val sMinor = minorVersion(version)
    tMajor > sMajor || (tMajor == sMajor && tMinor >= sMinor)
  }

  /**
   * Convert a time parameter such as (50s, 100ms, or 250us) to microseconds for internal use. If
   * no suffix is provided, the passed number is assumed to be in ms.
   */
  def timeStringAsMs(str: String): Long = {
    Utils.timeStringAsMs(str)
  }

  /**
   * Return a well-formed URI for the file described by a user input string.
   *
   * If the supplied path does not contain a scheme, or is a relative path, it will be
   * converted into an absolute path with a file:// scheme.
   */
  def resolveURI(path: String): URI = {
    Utils.resolveURI(path)
  }

  /**
   * Get the path of a temporary directory.  Spark's local directories can be configured through
   * multiple settings, which are used with the following precedence:
   *
   *   - If called from inside of a YARN container, this will return a directory chosen by YARN.
   *   - If the SPARK_LOCAL_DIRS environment variable is set, this will return a directory from it.
   *   - Otherwise, if the spark.local.dir is set, this will return a directory from it.
   *   - Otherwise, this will return java.io.tmpdir.
   *
   * Some of these configuration options might be lists of multiple paths, but this method will
   * always return a single directory.
   */
  def getLocalDir(conf: SparkConf): String = {
    Utils.getLocalDir(conf)
  }

  /**
   * Split a string of potentially quoted arguments from the command line the way that a shell
   * would do it to determine arguments to a command. For example, if the string is 'a "b c" d',
   * then it would be parsed as three arguments: 'a', 'b c' and 'd'.
   */
  def splitCommandString(s: String): Seq[String] = {
    Utils.splitCommandString(s)
  }

  /**
   * Replaces all the {{APP_ID}} occurrences with the App Id.
   */
  def substituteAppId(opt: String, appId: String): String = {
    opt.replace("{{APP_ID}}", appId)
  }

  /**
   * Escapes a string for inclusion in a command line executed by Yarn. Yarn executes commands
   * using either
   *
   * (Unix-based) `bash -c "command arg1 arg2"` and that means plain quoting doesn't really work.
   * The argument is enclosed in single quotes and some key characters are escaped.
   *
   * @param arg A single argument.
   *
   * @return Argument quoted for execution via Yarn's generated shell script.
   */
  def escapeForShell(arg: String): String = {
    val escaped = new StringBuilder("'")
    arg.foreach {
      case '$' => escaped.append("\\$")
      case '"' => escaped.append("\\\"")
      case '\'' => escaped.append("'\\''")
      case c => escaped.append(c)
    }
    escaped.append("'").toString()
  }

  /** Load properties present in the given file. */
  def getPropertiesFromFile(filename: String): Map[String, String] = {
    Utils.getPropertiesFromFile(filename)
  }

  def setActiveSparkContext(sc: SparkContext): Unit = {
    SparkContext.setActiveContext(sc, allowMultipleContexts = true)
  }

  /**
   * Get and set Kyuubi Jar First ClassLoader
   */
  def getAndSetKyuubiFirstClassLoader: MutableURLClassLoader = {
    val url = this.getClass.getProtectionDomain.getCodeSource.getLocation
    val loader = new ChildFirstURLClassLoader(
      Array(url),
      Thread.currentThread.getContextClassLoader)
    Thread.currentThread.setContextClassLoader(loader)
    info(s"Kyuubi first classloader is set to $url")
    loader
  }

  /** Determines whether the provided class is loadable in the current thread. */
  def classIsLoadable(clazz: String): Boolean = {
    Try { Class.forName(clazz, false, getContextOrSparkClassLoader) }.isSuccess
  }

  /**
   * Generate proper configurations before server starts
   * @param conf the default [[SparkConf]]
   */
  def setupCommonConfig(conf: SparkConf): Unit = {
    conf.setAppName("Kyuubi Server")
    // avoid max port retries reached
    conf.set(SPARK_UI_PORT, SPARK_UI_PORT_DEFAULT)
    conf.set(MULTIPLE_CONTEXTS, MULTIPLE_CONTEXTS_DEFAULT)
    conf.set(CATALOG_IMPL, CATALOG_IMPL_DEFAULT)
    // For the server itself the deploy mode could be either client or cluster,
    // but for the later [[SparkContext]] must be set to client mode
    conf.set(DEPLOY_MODE, DEPLOY_MODE_DEFAULT)
    // The delegation token store implementation. Set to MemoryTokenStore always.
    conf.set("spark.hadoop.hive.cluster.delegation.token.store.class",
      "org.apache.hadoop.hive.thrift.MemoryTokenStore")

    conf.getOption(METASTORE_JARS) match {
      case None | Some("builtin") =>
      case _ =>
        conf.set(METASTORE_JARS, "builtin")
        info(s"Kyuubi prefer $METASTORE_JARS to be builtin ones")
    }
    // Set missing Kyuubi configs to SparkConf
    KyuubiConf.getAllDefaults.foreach(kv => conf.setIfMissing(kv._1, kv._2))

    conf.setIfMissing(SPARK_LOCAL_DIR, conf.get(KyuubiConf.BACKEND_SESSION_LOCAL_DIR.key))
    conf.setIfMissing(GC_INTERVAL, GC_INTERVAL_DEFAULT)
    if (UserGroupInformation.isSecurityEnabled) {
      // Spark SQL does not use its internal ORC implementation to r/w hive orc tables by default,
      // but use hive serde which will cause token expiration issues while getting file splits.
      // So we force to set CONVERT_METASTORE_ORC to true if we are using Kyuubi with kerberized
      // hadoop clusters.
      // see https://github.com/yaooqinn/kyuubi/issues/153
      conf.set(CONVERT_METASTORE_ORC, "true")
    } else {
      // If the cluster is not secured, firstly we respect user's choice if the explicitly set, and
      // take true as default value for better performance if users are not concerned.
      conf.setIfMissing(CONVERT_METASTORE_ORC, "true")
    }
    conf.setIfMissing(ORC_IMPL, ORC_IMPL_DEFAULT)
    conf.setIfMissing(ORC_VECTORIZED_READER_ENABLED, "true")

    if (UserGroupInformation.isSecurityEnabled) {
      conf.setIfMissing(HDFS_CLIENT_CACHE, HDFS_CLIENT_CACHE_DEFAULT)
      conf.setIfMissing(FILE_CLIENT_CACHE, FILE_CLIENT_CACHE_DEFAULT)
      // If you are using Kyuubi against kerberized HDFS, you will run into HDFS_DELEGATION_TOKEN
      // expiration in some particular sql queries. This exception is usually caught in
      // HadoopRDD.getPartitions, where the JobConf has no Credentials because it is generated by
      // Configuration, and the UGI.getCurrentUser contains only the oldest tokens which are
      // destined to expire. The reason seems to be parallel listing UnionRDD's sub RDDs using a
      // ForkJoinPool which points to another calling context. Turn off parallel listing seems
      // to be a solution to this issue.
      conf.setIfMissing(RDD_PAR_LISTING, Int.MaxValue.toString)

      val sparkTokenProviders = List("hdfs", "hadoopfs", "hive", "hbase")
      val tokenProviderPattens = List(
        "spark.yarn.security.tokens.%s.enabled",
        "spark.yarn.security.credentials.%s.enabled",
        "spark.security.credentials.%s.enabled")
      // Set kyuubi credential renewer on, if we do not explicitly turn it off.
      tokenProviderPattens.map(_.format("kyuubi")).foreach(conf.setIfMissing(_, "true"))
      // Force to turn off Spark's internal token providers, because all useful works will be done
      // in KyuubiServiceCredentialProvider, and those ones in Spark always have impersonation
      // issue while renew tokens
      sparkTokenProviders.foreach  { service =>
          tokenProviderPattens.map(_.format(service)).foreach(conf.set(_, "false"))
      }

      // We should obey our client side hadoop settings while running Kyuubi towards HDFS
      // federations with maybe only on Yarn cluster
      // see https://github.com/apache/spark/pull/24120
      val hadoopConf = newConfiguration(conf)
      Option(hadoopConf.get("fs.defaultFS"))
        .foreach(conf.setIfMissing("spark.hadoop.fs.defaultFS", _))
    }
  }

  @tailrec
  def findCause(t: Throwable): Throwable = t match {
    case e: UndeclaredThrowableException =>
      if (e.getCause != null) findCause(e.getCause) else e
    case e: InvocationTargetException =>
      if (e.getCause != null) findCause(e.getCause) else e
    case e: Throwable => e
  }
}
