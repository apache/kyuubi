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

package org.apache.kyuubi.util

import org.apache.kyuubi.KyuubiFunSuite
import org.apache.kyuubi.config.KyuubiConf

class ClassUtilsSuite extends KyuubiFunSuite {

  private val _conf = KyuubiConf()

  test("create instance with zero-arg arg") {
    val instance = ClassUtils.createInstance[SomeProvider](
      "org.apache.kyuubi.util.ProviderA",
      classOf[SomeProvider],
      _conf)

    assert(instance != null)
    assert(instance.isInstanceOf[SomeProvider])
    assert(instance.isInstanceOf[ProviderA])
  }

  test("create instance with kyuubi conf") {
    val instance = ClassUtils.createInstance[SomeProvider](
      "org.apache.kyuubi.util.ProviderB",
      classOf[SomeProvider],
      _conf)
    assert(instance != null)
    assert(instance.isInstanceOf[SomeProvider])
    assert(instance.isInstanceOf[ProviderB])
    assert(instance.asInstanceOf[ProviderB].getConf != null)
  }

  test("create instance of inherited class with kyuubi conf") {
    val instance = ClassUtils.createInstance[SomeProvider](
      "org.apache.kyuubi.util.ProviderC",
      classOf[SomeProvider],
      _conf)
    assert(instance != null)
    assert(instance.isInstanceOf[SomeProvider])
    assert(instance.isInstanceOf[ProviderB])
    assert(instance.isInstanceOf[ProviderC])
    assert(instance.asInstanceOf[ProviderC].getConf != null)
  }

}

trait SomeProvider {}

class ProviderA extends SomeProvider {}

class ProviderB(conf: KyuubiConf) extends SomeProvider {
  def getConf: KyuubiConf = conf
}

class ProviderC(conf: KyuubiConf) extends ProviderB(conf) {}
