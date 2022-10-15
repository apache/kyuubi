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

package org.apache.kyuubi.engine.flink

import scala.collection.JavaConverters._

import org.apache.flink.client.cli.{CustomCommandLine, DefaultCLI}
import org.apache.flink.configuration.{Configuration, RestOptions}
import org.apache.flink.runtime.minicluster.{MiniCluster, MiniClusterConfiguration}
import org.apache.flink.table.client.gateway.context.DefaultContext

import org.apache.kyuubi.{KyuubiFunSuite, Utils}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.engine.flink.util.TestUserClassLoaderJar

trait WithFlinkSQLEngine extends KyuubiFunSuite {

  protected val flinkConfig = new Configuration()
  protected var miniCluster: MiniCluster = _
  protected var engine: FlinkSQLEngine = _
  // conf will be loaded until start flink engine
  def withKyuubiConf: Map[String, String]
  val kyuubiConf: KyuubiConf = FlinkSQLEngine.kyuubiConf

  protected var connectionUrl: String = _

  protected val GENERATED_UDF_CLASS: String = "LowerUDF"

  protected val GENERATED_UDF_CODE: String =
    s"""
      public class $GENERATED_UDF_CLASS extends org.apache.flink.table.functions.ScalarFunction {
        public String eval(String str) {
          return str.toLowerCase();
        }
      }
     """

  override def beforeAll(): Unit = {
    startMiniCluster()
    startFlinkEngine()
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    super.afterAll()
    stopFlinkEngine()
    miniCluster.close()
  }

  def startFlinkEngine(): Unit = {
    withKyuubiConf.foreach { case (k, v) =>
      System.setProperty(k, v)
      kyuubiConf.set(k, v)
    }
    val udfJar = TestUserClassLoaderJar.createJarFile(
      Utils.createTempDir("test-jar").toFile,
      "test-classloader-udf.jar",
      GENERATED_UDF_CLASS,
      GENERATED_UDF_CODE)
    val engineContext = new DefaultContext(
      List(udfJar.toURI.toURL).asJava,
      flinkConfig,
      List[CustomCommandLine](new DefaultCLI).asJava)
    FlinkSQLEngine.startEngine(engineContext)
    engine = FlinkSQLEngine.currentEngine.get
    connectionUrl = engine.frontendServices.head.connectionUrl
  }

  def stopFlinkEngine(): Unit = {
    if (engine != null) {
      engine.stop()
      engine = null
    }
  }

  private def startMiniCluster(): Unit = {
    val cfg = new MiniClusterConfiguration.Builder()
      .setConfiguration(flinkConfig)
      .setNumSlotsPerTaskManager(1)
      .build
    miniCluster = new MiniCluster(cfg)
    miniCluster.start()
    flinkConfig.setString(RestOptions.ADDRESS, miniCluster.getRestAddress.get().getHost)
    flinkConfig.setInteger(RestOptions.PORT, miniCluster.getRestAddress.get().getPort)
  }

  protected def getJdbcUrl: String = s"jdbc:hive2://$connectionUrl/;"

}
