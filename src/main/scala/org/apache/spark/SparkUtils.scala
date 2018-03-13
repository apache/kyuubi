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

import org.apache.hadoop.conf.Configuration
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.util.{ShutdownHookManager, Utils}
import org.slf4j.Logger

/**
 * Wrapper for [[Utils]] and [[SparkHadoopUtil]]
 */
object SparkUtils {

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
}
