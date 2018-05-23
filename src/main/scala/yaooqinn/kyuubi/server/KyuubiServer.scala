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

package yaooqinn.kyuubi.server

import java.io.File
import java.util.concurrent.atomic.AtomicBoolean

import org.apache.hadoop.security.UserGroupInformation
import org.apache.spark.{KyuubiConf, KyuubiSparkUtil, SparkConf}

import yaooqinn.kyuubi._
import yaooqinn.kyuubi.ha.HighAvailabilityUtils
import yaooqinn.kyuubi.service.CompositeService
import yaooqinn.kyuubi.utils.HadoopUtils

/**
 * Main entrance of Kyuubi Server
 */
private[kyuubi] class KyuubiServer private(name: String)
  extends CompositeService(name) with Logging {

  private[this] var _beService: BackendService = _
  def beService: BackendService = _beService
  private[this] var _feService: FrontendService = _
  def feService: FrontendService = _feService

  private[this] val started = new AtomicBoolean(false)

  def this() = this(classOf[KyuubiServer].getSimpleName)

  override def init(conf: SparkConf): Unit = synchronized {
    this.conf = conf
    _beService = new BackendService()
    _feService = new FrontendService(_beService)
    addService(_beService)
    addService(_feService)
    super.init(conf)
    KyuubiSparkUtil.addShutdownHook {
      () => this.stop()
    }
  }

  override def start(): Unit = {
    super.start()
    started.set(true)
  }

  override def stop(): Unit = {
    if (started.getAndSet(false)) {
      super.stop()
    }
  }
}

object KyuubiServer extends Logging {

  def main(args: Array[String]): Unit = {
    KyuubiSparkUtil.initDaemon(logger)
    validate()
    val conf = new SparkConf(loadDefaults = true)
    setupCommonConfig(conf)

    try {
      val server = new KyuubiServer()
      server.init(conf)
      server.start()
      info(server.getName + " started!")
      if (HighAvailabilityUtils.isSupportDynamicServiceDiscovery(conf)) {
        info(s"HA mode: start to add this ${server.getName} instance to Zookeeper...")
        HighAvailabilityUtils.addServerInstanceToZooKeeper(server)
      }
    } catch {
      case e: Exception =>
        error("Error starting Kyuubi Server", e)
        System.exit(-1)
    }
  }

  /**
   * Generate proper configurations before server starts
   * @param conf the default [[SparkConf]]
   */
  private[kyuubi] def setupCommonConfig(conf: SparkConf): Unit = {
    // will be overwritten later for each SparkContext
    conf.setAppName(classOf[KyuubiServer].getSimpleName)
    // avoid max port retries reached
    conf.set(KyuubiSparkUtil.SPARK_UI_PORT, KyuubiSparkUtil.SPARK_UI_PORT_DEFAULT)
    conf.set(KyuubiSparkUtil.MULTIPLE_CONTEXTS, KyuubiSparkUtil.MULTIPLE_CONTEXTS_DEFAULT)
    conf.set(KyuubiSparkUtil.CATALOG_IMPL, KyuubiSparkUtil.CATALOG_IMPL_DEFAULT)
    // For the server itself the deploy mode could be either client or cluster,
    // but for the later [[SparkContext]] must be set to client mode
    conf.set(KyuubiSparkUtil.DEPLOY_MODE, KyuubiSparkUtil.DEPLOY_MODE_DEFAULT)
    // The delegation token store implementation. Set to MemoryTokenStore always.
    conf.set("spark.hadoop.hive.cluster.delegation.token.store.class",
      "org.apache.hadoop.hive.thrift.MemoryTokenStore")

    // Spark' hive metastore client uses Isolated ClassLoader for instantiation, which is conflict
    // with [[KyuubiFirstClassLoader]] while using default builtin hive jars. This may be a Spark's
    // bug, so we bypassing it here by directly setting it to the builtin dir.
    conf.getOption(KyuubiSparkUtil.METASTORE_JARS) match {
      case None | Some("builtin") =>
        val metastoreJars = KyuubiSparkUtil.SPARK_JARS_DIR + File.separator + "*"
        info(s"Using Spark's bulitin ones for ${KyuubiSparkUtil.METASTORE_JARS}")
        conf.set(KyuubiSparkUtil.METASTORE_JARS, metastoreJars)
      case Some(other) =>
        info(s"Using user specified ${KyuubiSparkUtil.METASTORE_JARS}=$other")
    }
    // Set missing Kyuubi configs to SparkConf
    KyuubiConf.getAllDefaults.foreach(kv => conf.setIfMissing(kv._1, kv._2))

    conf.setIfMissing(
      KyuubiSparkUtil.SPARK_LOCAL_DIR, conf.get(KyuubiConf.BACKEND_SESSION_LOCAL_DIR.key))
  }

  private[kyuubi] def validate(): Unit = {
    if (KyuubiSparkUtil.majorVersion(KyuubiSparkUtil.SPARK_VERSION) < 2) {
      throw new KyuubiServerException(s"${KyuubiSparkUtil.SPARK_VERSION} is too old for Kyuubi" +
        s" Server.")
    }

    info(s"Starting Kyuubi Server version ${KYUUBI_VERSION} compiled with Spark version:" +
      s" ${SPARK_COMPILE_VERSION}, and run with Spark Version ${KyuubiSparkUtil.SPARK_VERSION}")
    if (SPARK_COMPILE_VERSION != KyuubiSparkUtil.SPARK_VERSION) {
      warn(s"Running Kyuubi with Spark ${KyuubiSparkUtil.SPARK_VERSION}, which is compiled by" +
        s" $SPARK_COMPILE_VERSION. PLEASE be aware of possible incompatibility issues")
    }

    if (UserGroupInformation.isSecurityEnabled) {
      if (HadoopUtils.isProxyUser(UserGroupInformation.getCurrentUser)) {
        warn(s"Kyuubi Server itself is started by proxying. PLEASE be aware that Kyuubi now can " +
          s"not impersonating and only for ${UserGroupInformation.getCurrentUser.
            getShortUserName} to connect")
      }
    }
  }
}
