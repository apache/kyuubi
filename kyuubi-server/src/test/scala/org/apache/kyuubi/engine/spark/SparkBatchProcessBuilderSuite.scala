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

package org.apache.kyuubi.engine.spark

import java.util.UUID

import scala.concurrent.duration._

import org.apache.kyuubi.KyuubiFunSuite
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.server.MiniYarnService
import org.apache.kyuubi.server.api.v1.BatchRequest

class SparkBatchProcessBuilderSuite extends KyuubiFunSuite {
  private val conf = KyuubiConf().set("kyuubi.on", "off")
  private var miniYarnService: MiniYarnService = _

  override def beforeAll(): Unit = {
    miniYarnService = new MiniYarnService()
    miniYarnService.initialize(new KyuubiConf(false))
    miniYarnService.start()
    conf.set(
      s"${KyuubiConf.KYUUBI_ENGINE_ENV_PREFIX}.HADOOP_CONF_DIR",
      miniYarnService.getHadoopConfDir)
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    if (miniYarnService != null) {
      miniYarnService.stop()
      miniYarnService = null
    }
    super.afterAll()
  }

  test("spark batch process builder") {
    val sparkProcessBuilder = new SparkProcessBuilder("kyuubi", conf)

    val batchRequest = BatchRequest(
      "spark",
      sparkProcessBuilder.mainResource.get,
      "kyuubi",
      sparkProcessBuilder.mainClass,
      "spark-batch-submission",
      Map("spark.master" -> "yarn"),
      Seq.empty[String])

    val builder = new SparkBatchProcessBuilder(
      batchRequest.proxyUser,
      conf,
      UUID.randomUUID().toString,
      batchRequest)
    val proc = builder.start

    eventually(timeout(3.minutes), interval(500.milliseconds)) {
      val applicationIdAndUrl = builder.getApplicationIdAndUrl()
      assert(applicationIdAndUrl.isDefined)
      assert(applicationIdAndUrl.exists(_._1.startsWith("application_")))
      assert(applicationIdAndUrl.exists(_._2.nonEmpty))
    }
    proc.destroyForcibly()
  }
}
