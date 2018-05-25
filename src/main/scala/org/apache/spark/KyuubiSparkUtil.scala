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

import scala.util.matching.Regex

import org.apache.hadoop.conf.Configuration
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.util.{ShutdownHookManager, Utils, VersionUtils}
import org.slf4j.Logger

import yaooqinn.kyuubi.Logging

/**
 * Wrapper for [[Utils]] and [[SparkHadoopUtil]]
 */
object KyuubiSparkUtil extends Logging {
  // PREFIXES
  val SPARK_PREFIX = "spark."
  private[this] val YARN_PREFIX = "yarn."
  private[this] val HADOOP_PRFIX = "hadoop."
  val SPARK_HADOOP_PREFIX = SPARK_PREFIX + HADOOP_PRFIX
  private[this] val DRIVER_PREFIX = "driver."
  private[this] val UI_PREFIX = "ui."
  private[this] val SQL_PREFIX = "sql."
  private[this] val HIVE_PREFIX = "hive."

  val SPARK_HOME = System.getenv("SPARK_HOME")
  val SPARK_JARS_DIR = SPARK_HOME + File.separator + "jars"

  val KEYTAB = SPARK_PREFIX + YARN_PREFIX + "keytab"
  val PRINCIPAL = SPARK_PREFIX + YARN_PREFIX + "principal"
  val DRIVER_BIND_ADDR = SPARK_PREFIX + DRIVER_PREFIX + "bindAddress"

  val SPARK_UI_PORT = SPARK_PREFIX + UI_PREFIX + "port"
  val SPARK_UI_PORT_DEFAULT = "0"

  val MULTIPLE_CONTEXTS = SPARK_PREFIX + DRIVER_PREFIX + "allowMultipleContexts"
  val MULTIPLE_CONTEXTS_DEFAULT = "true"

  val CATALOG_IMPL = SPARK_PREFIX + SQL_PREFIX + "catalogImplementation"
  val CATALOG_IMPL_DEFAULT = "hive"

  val DEPLOY_MODE = SPARK_PREFIX + "submit.deployMode"
  val DEPLOY_MODE_DEFAULT = "client"

  val METASTORE_JARS = SPARK_PREFIX + SQL_PREFIX + HIVE_PREFIX + "metastore.jars"

  val SPARK_LOCAL_DIR = SPARK_PREFIX + "local.dir"

  val HIVE_VAR_PREFIX: Regex = """set:hivevar:([^=]+)""".r
  val USE_DB: Regex = """use:([^=]+)""".r
  val QUEUE = SPARK_PREFIX + YARN_PREFIX + "queue"
  val DEPRECATED_QUEUE = "mapred.job.queue.name"

  // Runtime Spark Version
  val SPARK_VERSION = org.apache.spark.SPARK_VERSION

  def addShutdownHook(f: () => Unit): Unit = {
    ShutdownHookManager.addShutdownHook(f)
  }

  def initDaemon(log: Logger): Unit = {
    Utils.initDaemon(log)
  }

  def getJobGroupIDKey(): String = SparkContext.SPARK_JOB_GROUP_ID

  def exceptionString(e: Throwable): String = {
    Utils.exceptionString(e)
  }

  def getCurrentUserName(): String = {
    Utils.getCurrentUserName()
  }

  def getContextOrSparkClassLoader(): ClassLoader = {
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
}
