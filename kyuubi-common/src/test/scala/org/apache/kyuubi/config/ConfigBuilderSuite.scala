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

package org.apache.kyuubi.config

import org.apache.kyuubi.KyuubiFunSuite

class ConfigBuilderSuite extends KyuubiFunSuite {

  test("int config") {
    val intConf = ConfigBuilder("kyuubi.int.conf")
      .intConf
      .createWithDefault(0)
    assert(intConf.key === "kyuubi.int.conf")
    assert(intConf.defaultVal.get === 0)
  }

  test("long config") {
    val longConf = ConfigBuilder("kyuubi.long.conf")
      .longConf
      .createWithDefault(1)
    assert(longConf.key === "kyuubi.long.conf")
    assert(longConf.defaultVal.get === 1)
  }

  test("double config") {
    val doubleConf = ConfigBuilder("kyuubi.double.conf")
      .doubleConf
      .createWithDefault(2.0)
    assert(doubleConf.key === "kyuubi.double.conf")
    assert(doubleConf.defaultVal.get === 2.0)
  }

  test("boolean config") {
    val booleanConf = ConfigBuilder("kyuubi.boolean.conf")
      .booleanConf
      .createWithDefault(false)
    assert(booleanConf.key === "kyuubi.boolean.conf")
    assert(!booleanConf.defaultVal.get)
    val kyuubiConf = KyuubiConf().set(booleanConf.key, "invalid conf")
    val e = intercept[IllegalArgumentException](kyuubiConf.get(booleanConf))
    assert(e.getMessage === "kyuubi.boolean.conf should be boolean, but was invalid conf")
  }

  test("string config") {
    val stringConf = ConfigBuilder("kyuubi.string.conf")
      .stringConf
      .createWithDefault("kent, yao")
    assert(stringConf.key === "kyuubi.string.conf")
    assert(stringConf.defaultVal.get === "kent, yao")
    val sequenceConf = ConfigBuilder("kyuubi.sequence.conf")
      .stringConf
      .toSequence()
      .createWithDefault(Nil)
    assert(sequenceConf.defaultVal.get.isEmpty)
    val kyuubiConf = KyuubiConf().set(sequenceConf.key, "kyuubi,kent")
    assert(kyuubiConf.get(sequenceConf) === Seq("kyuubi", "kent"))
  }

  test("time config") {
    val timeConf = ConfigBuilder("kyuubi.time.config")
      .timeConf
      .createWithDefault(3)
    assert(timeConf.key === "kyuubi.time.config")
    assert(timeConf.defaultVal.get === 3)
    val kyuubiConf = KyuubiConf().set(timeConf.key, "invalid")
    val e = intercept[IllegalArgumentException](kyuubiConf.get(timeConf))
    assert(e.getMessage startsWith "The formats accepted are 1) based on the ISO-8601")
  }
}
