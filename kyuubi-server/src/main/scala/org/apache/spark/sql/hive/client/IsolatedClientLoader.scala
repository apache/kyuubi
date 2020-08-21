/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.hive.client

import java.net.URL

import scala.util.Try

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hive.conf.HiveConf.ConfVars
import org.apache.kyuubi.Logging
import org.apache.spark.{KyuubiSparkUtil, SparkConf}
import org.apache.spark.sql.internal.NonClosableMutableURLClassLoader
import org.apache.spark.util.MutableURLClassLoader

/**
 * A Hacking Class for [[IsolatedClientLoader]] to be not isolated
 */
private[hive] object IsolatedClientLoader {
  def hiveVersion(version: String): HiveVersion = version match {
    case "12" | "0.12" | "0.12.0" => hive.v12
    case "13" | "0.13" | "0.13.0" | "0.13.1" => hive.v13
    case "14" | "0.14" | "0.14.0" => hive.v14
    case "1.0" | "1.0.0" => hive.v1_0
    case "1.1" | "1.1.0" => hive.v1_1
    case "1.2" | "1.2.0" | "1.2.1" | "1.2.2" => hive.v1_2
  }
}

private[hive] class IsolatedClientLoader(
    val version: HiveVersion,
    val sparkConf: SparkConf,
    val hadoopConf: Configuration,
    val execJars: Seq[URL] = Seq.empty,
    val config: Map[String, String] = Map.empty,
    val isolationOn: Boolean = true,
    val sharesHadoopClasses: Boolean = true,
    val rootClassLoader: ClassLoader = ClassLoader.getSystemClassLoader.getParent.getParent,
    val baseClassLoader: ClassLoader = Thread.currentThread().getContextClassLoader,
    val sharedPrefixes: Seq[String] = Seq.empty,
    val barrierPrefixes: Seq[String] = Seq.empty)
  extends Logging {

  import KyuubiSparkUtil._

  // Check to make sure that the root classloader does not know about Hive.
  assert(Try(rootClassLoader.loadClass("org.apache.hadoop.hive.conf.HiveConf")).isFailure)

  /**
   * (Kent Yao) Different with Spark internal which use an isolated classloader to support different
   * Hive versions, Kyuubi believe that the hive 1.2.1 is capable to support 1.2 or higher version
   * Hive metastore servers and the elder hive client versions are not worth to support.
   *
   * ANOTHER reason here we close the isolation is because Spark don't expose authorization
   * functions in [[HiveClient]], which is unable to invoke these methods in different classloaders
   *
   * Besides, [[HiveClient]] in normal Spark applications is globally one instance, so this
   * classloader could/should be non-closeable. But in Kyuubi, this is a session level object
   * associated with one KyuubiSession/SparkSession, thus, this classloader should be closeable to
   * support class unloading.
   *
   * This classloader is a special URLClassLoader that exposes the addURL method.
   * So, when we add jar, we can add this new jar directly through the addURL method
   * instead of stacking a new URLClassLoader on top of it.
   */
  private[hive] val classLoader: MutableURLClassLoader = {
    new MutableURLClassLoader(Array.empty, baseClassLoader)
  }

  private[hive] def addJar(path: URL): Unit = {
    classLoader.addURL(path)
  }

  /** The isolated client interface to Hive. */
  private[hive] def createClient(): HiveClient = synchronized {

    val ctor = classOf[HiveClientImpl].getConstructors.head
    if (majorVersion(SPARK_VERSION) == 2 && minorVersion(SPARK_VERSION) > 3) {
      val warehouseDir = Option(hadoopConf.get(ConfVars.METASTOREWAREHOUSE.varname))
      ctor.newInstance(
        version,
        warehouseDir,
        sparkConf,
        hadoopConf,
        config,
        classLoader,
        this).asInstanceOf[HiveClientImpl]
    } else {
      ctor.newInstance(
        version,
        sparkConf,
        hadoopConf,
        config,
        classLoader,
        this).asInstanceOf[HiveClientImpl]
    }

  }

  /**
   * The place holder for shared Hive client for all the HiveContext sessions (they share an
   * IsolatedClientLoader).
   */
  private[hive] var cachedHive: Any = null
}
