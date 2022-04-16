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

import scala.concurrent.duration.DurationInt

import org.apache.hadoop.yarn.client.api.YarnClient
import org.scalatestplus.mockito.MockitoSugar.mock

import org.apache.kyuubi.{Utils, WithKyuubiServerOnYarn}
import org.apache.kyuubi.config.KyuubiConf

class SparkProcessBuilderOnYarnSuite extends WithKyuubiServerOnYarn {

  override protected val kyuubiServerConf: KyuubiConf = KyuubiConf()

  override protected val connectionConf: Map[String, String] = Map(
    "spark.master" -> "yarn",
    "spark.executor.instances" -> "1")

  test("test kill application") {
    val engineRefId = UUID.randomUUID().toString

    conf.set(
      SparkProcessBuilder.TAG_KEY,
      conf.getOption(SparkProcessBuilder.TAG_KEY).map(_ + ",").getOrElse("") +
        "KYUUBI," + engineRefId)
    val builder = new SparkProcessBuilder(Utils.currentUser, conf)
    val proc = builder.start
    eventually(timeout(3.minutes), interval(1.seconds)) {
      val killMsg = builder.killApplication(Left(engineRefId))
      assert(killMsg.contains(s"tagged with $engineRefId successfully."))
    }
    proc.destroyForcibly()

    val pb1 = new FakeSparkProcessBuilder(conf.clone) {
      override protected def env: Map[String, String] = Map()
      override def getYarnClient: YarnClient = mock[YarnClient]
    }
    val exit1 = pb1.killApplication(Left(engineRefId))
    assert(exit1.equals(s"There are no Application tagged with $engineRefId," +
      s" please kill it manually."))

    val pb2 = new FakeSparkProcessBuilder(conf.clone) {
      override protected def env: Map[String, String] = Map()
      override def getYarnClient: YarnClient = mock[YarnClient]
    }
    pb2.conf.set("spark.master", "local")
    val exit2 = pb2.killApplication(Left(engineRefId))
    assert(exit2.equals("Kill Application only works with YARN, please kill it manually." +
      s" Application tagged with $engineRefId"))
  }

}
