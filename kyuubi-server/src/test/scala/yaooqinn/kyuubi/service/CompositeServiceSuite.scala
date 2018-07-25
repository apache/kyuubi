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

package yaooqinn.kyuubi.service

import org.apache.spark.{SparkConf, SparkFunSuite}

class CompositeServiceSuite extends SparkFunSuite {

  test("composite service test") {
    val s0 = new TestService1("s0")
    val s1 = new TestService1("s1") {
      override def stop(): Unit = {
        super.stop()
        throw new ServiceException("s2 stop failure")
      }
    }
    val s2 = new TestService1("s2")
    val s3 = new AbstractService("s3") {
      override def start(): Unit = {
        throw new ServiceException("s3 start failure")
      }
    }
    val cs1 = new CompositeService("cs1") {
      addService(s0)
      addService(s1)
    }
    val cs2 = new CompositeService("cs2") {
      addService(s2)
      addService(s3)
    }
    assert(cs1.getServices.size === 2)
    val e0 = intercept[ServiceException](cs1.start())
    assert(e0.getCause.isInstanceOf[IllegalStateException])
    val conf = new SparkConf()
    cs1.init(conf)
    assert(s0.getServiceState === State.INITED)
    assert(s1.getServiceState === State.INITED)
    assert(cs1.getServiceState === State.INITED)

    cs1.start()
    assert(s0.getServiceState === State.STARTED)
    assert(s1.getServiceState === State.STARTED)
    assert(cs1.getServiceState === State.STARTED)

    cs1.stop()
    assert(s0.getServiceState === State.STOPPED)
    assert(s1.getServiceState === State.STOPPED)
    assert(cs1.getServiceState === State.STOPPED)
    cs1.stop()

    cs2.init(conf)
    val e2 = intercept[ServiceException](cs2.start())
    assert(e2.getMessage === "Failed to Start cs2")
    assert(e2.getCause.getMessage === "s3 start failure")
    assert(s2.getServiceState === State.STOPPED)
    assert(s3.getServiceState === State.INITED)
    assert(cs2.getServiceState === State.INITED)
    cs2.stop()

    val cs3 = new CompositeService("cs3")
    cs3.stop()
    intercept[IllegalStateException](cs3.start())
    cs3.init(conf)
    cs3.start()
  }
}
