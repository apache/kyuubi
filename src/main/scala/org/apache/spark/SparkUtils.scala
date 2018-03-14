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
import java.lang.reflect.UndeclaredThrowableException
import java.security.PrivilegedExceptionAction

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.security.UserGroupInformation
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.util.{ShutdownHookManager, Utils}
import org.slf4j.Logger

/**
 * Wrapper for [[Utils]] and [[SparkHadoopUtil]]
 */
object SparkUtils {

  val SPARK_PREFIX = "spark."
  val YARN_PREFIX = "yarn."

  val FILESYSTEMS_TO_ACCESS = SPARK_PREFIX + YARN_PREFIX + "access.hadoopFileSystems"
  val KEYTAB = SPARK_PREFIX + YARN_PREFIX + "keytab"
  val NAMENODES_TO_ACCESS = SPARK_PREFIX + YARN_PREFIX + "access.namenodes"
  val PRINCIPAL = SPARK_PREFIX + YARN_PREFIX + "principal"
  val STAGING_DIR = SPARK_PREFIX + YARN_PREFIX + "stagingDir"

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

  def getLocalDir(conf: SparkConf): String = {
    Utils.getLocalDir(conf)
  }

  def createTempDir(
      root: String = System.getProperty("java.io.tmpdir"),
      namePrefix: String = "spark"): File = {
    Utils.createTempDir(root, namePrefix)
  }

  def getUserJars(conf: SparkConf): Seq[String] = {
    Utils.getUserJars(conf)
  }

  def newConfiguration(conf: SparkConf): Configuration = {
    SparkHadoopUtil.get.newConfiguration(conf)
  }

  /**
   * Run some code as the real logged in user (which may differ from the current user, for
   * example, when using proxying).
   */
  def doAsRealUser[T](fn: => T): T = {
    val currentUser = UserGroupInformation.getCurrentUser
    val realUser = Option(currentUser.getRealUser).getOrElse(currentUser)

    // For some reason the Scala-generated anonymous class ends up causing an
    // UndeclaredThrowableException, even if you annotate the method with @throws.
    try {
      realUser.doAs(new PrivilegedExceptionAction[T]() {
        override def run(): T = fn
      })
    } catch {
      case e: UndeclaredThrowableException => throw Option(e.getCause).getOrElse(e)
    }
  }

  /** Executes the given block. Log non-fatal errors if any, and only throw fatal errors */
  def tryLogNonFatalError(block: => Unit): Unit = {
    Utils.tryLogNonFatalError(block)
  }
}
