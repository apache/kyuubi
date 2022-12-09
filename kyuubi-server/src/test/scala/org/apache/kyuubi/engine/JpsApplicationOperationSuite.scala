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

package org.apache.kyuubi.engine

import java.lang.management.ManagementFactory
import java.time.Duration
import java.util.{ServiceLoader, UUID}

import scala.collection.JavaConverters._
import scala.sys.process._

import org.scalatest.concurrent.PatienceConfiguration.Timeout
import org.scalatest.time.SpanSugar._

import org.apache.kyuubi.{KyuubiFunSuite, Utils}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf.SESSION_IDLE_TIMEOUT
import org.apache.kyuubi.engine.spark.SparkProcessBuilder

class JpsApplicationOperationSuite extends KyuubiFunSuite {
  private val operations = ServiceLoader.load(classOf[ApplicationOperation])
    .asScala.filter(_.getClass.isAssignableFrom(classOf[JpsApplicationOperation]))
  private val jps = operations.head
  jps.initialize(null)

  test("JpsApplicationOperation with jstat") {
    assert(jps.isSupported(None))
    assert(jps.isSupported(Some("local")))
    assert(!jps.killApplicationByTag(null)._1)
    assert(!jps.killApplicationByTag("have a space")._1)
    val currentProcess = ManagementFactory.getRuntimeMXBean.getName
    val currentPid = currentProcess.splitAt(currentProcess.indexOf("@"))._1

    new Thread {
      override def run(): Unit = {
        s"jstat -gcutil $currentPid 1000".!
      }
    }.start()

    eventually(Timeout(10.seconds)) {
      val desc1 = jps.getApplicationInfoByTag("sun.tools.jstat.Jstat")
      assert(desc1.id != null)
      assert(desc1.name != null)
      assert(desc1.state == ApplicationState.RUNNING)
    }

    jps.killApplicationByTag("sun.tools.jstat.Jstat")

    eventually(Timeout(10.seconds)) {
      val desc2 = jps.getApplicationInfoByTag("sun.tools.jstat.Jstat")
      assert(desc2.id == null)
      assert(desc2.name == null)
      assert(desc2.state == ApplicationState.NOT_FOUND)
    }
  }

  test("JpsApplicationOperation with spark local mode") {
    val user = Utils.currentUser
    val id = UUID.randomUUID().toString
    val conf = new KyuubiConf()
      .set("spark.abc", id)
      .set("spark.master", "local")
      .set(SESSION_IDLE_TIMEOUT, Duration.ofMinutes(3).toMillis)
    val builder = new SparkProcessBuilder(user, conf)
    builder.start

    assert(jps.isSupported(builder.clusterManager()))
    eventually(Timeout(10.seconds)) {
      val desc1 = jps.getApplicationInfoByTag(id)
      assert(desc1.id != null)
      assert(desc1.name != null)
      assert(desc1.state == ApplicationState.RUNNING)
      val response = jps.killApplicationByTag(id)
      assert(response._1, response._2)
      assert(response._2 startsWith "Succeeded to terminate:")
    }

    eventually(Timeout(10.seconds)) {
      val desc2 = jps.getApplicationInfoByTag(id)
      assert(desc2.id == null)
      assert(desc2.name == null)
      assert(desc2.state == ApplicationState.NOT_FOUND)
    }

    val response2 = jps.killApplicationByTag(id)
    assert(!response2._1)
    assert(response2._2 === ApplicationOperation.NOT_FOUND)
  }
}
