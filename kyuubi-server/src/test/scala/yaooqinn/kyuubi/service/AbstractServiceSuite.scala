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

class AbstractServiceSuite extends SparkFunSuite {

  test("abstract service test") {
    val serviceName = "test service"
    val service = new TestService1(serviceName)
    assert(service.getServiceState === State.NOT_INITED)
    assert(service.getConf === null)
    assert(service.getStartTime === 0L)
    assert(service.getName === serviceName)
    intercept[IllegalStateException](service.start())

    service.stop()
    assert(service.getServiceState === State.NOT_INITED)
    val conf = new SparkConf()
    service.init(conf)
    assert(service.getConf === conf)
    assert(service.getStartTime !== 0L)
    assert(service.getServiceState === State.INITED)
    service.stop()
    assert(service.getServiceState === State.INITED)
    service.start()
    assert(service.getStartTime !== 0L)
    assert(service.getServiceState === State.STARTED)
    service.stop()
    assert(service.getServiceState === State.STOPPED)
    service.stop()
    assert(service.getServiceState === State.STOPPED)
  }
}

class TestService1(name: String) extends AbstractService(name)