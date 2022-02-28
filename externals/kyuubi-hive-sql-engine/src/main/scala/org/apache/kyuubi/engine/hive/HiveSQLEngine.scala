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

package org.apache.kyuubi.engine.hive

import java.util.Properties
import java.util.concurrent.CountDownLatch

import scala.collection.JavaConverters._

import org.apache.commons.cli.{DefaultParser, Options}
import org.apache.hadoop.hive.conf.HiveConf

import org.apache.kyuubi.{Logging, Utils}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.engine.hive.HiveSQLEngine.countDownLatch
import org.apache.kyuubi.ha.HighAvailabilityConf.HA_ZK_CONN_RETRY_POLICY
import org.apache.kyuubi.ha.client.RetryPolicies
import org.apache.kyuubi.service.{AbstractBackendService, AbstractFrontendService, Serverable}
import org.apache.kyuubi.util.SignalRegister

class HiveSQLEngine extends Serverable("HiveSQLEngine") {
  override val backendService: AbstractBackendService = new HiveBackendService(this)
  override val frontendServices: Seq[AbstractFrontendService] =
    Seq(new HiveTBinaryFrontend(this))

  override def start(): Unit = {
    super.start()
    // Start engine self-terminating checker after all services are ready and it can be reached by
    // all servers in engine spaces.
    backendService.sessionManager.startTerminatingChecker(() => stop())
  }

  override protected def stopServer(): Unit = {
    countDownLatch.countDown()
  }
}

object HiveSQLEngine extends Logging {
  private val countDownLatch = new CountDownLatch(1)
  var currentEngine: Option[HiveSQLEngine] = None
  val hiveConf = new HiveConf()
  val kyuubiConf = new KyuubiConf()

  def main(args: Array[String]): Unit = {
    info(System.getProperty("java.class.path"))
    SignalRegister.registerLogger(logger)
    val properties = new HiveEngineOptionsProcessor().parse(args)

    kyuubiConf.setIfMissing(KyuubiConf.FRONTEND_THRIFT_BINARY_BIND_PORT, 0)
    kyuubiConf.setIfMissing(HA_ZK_CONN_RETRY_POLICY, RetryPolicies.N_TIME.toString)

    for ((k, v) <- properties.asScala) {
      kyuubiConf.set(k, v)
    }

    for ((k, v) <- kyuubiConf.getAll) {
      hiveConf.set(k, v)
    }

    if (hiveConf.getBoolVar(HiveConf.ConfVars.METASTOREURIS)) {}

    for (elem <- hiveConf.asScala) {
      info(elem.getKey + ", " + elem.getValue)
    }

    hiveConf.setIfUnset(
      "javax.jdo.option.ConnectionURL",
      "jdbc:derby:;databaseName=metastore_db;create=true")

    currentEngine = Some(new HiveSQLEngine())
    try {
      info(s"Starting ${currentEngine.get.getName}")
      currentEngine.get.initialize(kyuubiConf)
      currentEngine.get.start()
      Utils.addShutdownHook(() => currentEngine.foreach(_.stop()))
    } catch {
      case t: Throwable =>
        error(t)
        currentEngine.get.stop()
    }
    countDownLatch.await()
  }

  class HiveEngineOptionsProcessor {
    final private val options = {
      new Options().addOption(
        org.apache.commons.cli.Option.builder("c")
          .valueSeparator()
          .hasArgs
          .argName("property=value")
          .longOpt("hiveconf")
          .desc("Use value for given property")
          .build())
    }

    def parse(args: Array[String]): Properties = {
      val commandLine = new DefaultParser().parse(options, args)
      commandLine.getOptionProperties("hiveconf")
    }
  }
}
